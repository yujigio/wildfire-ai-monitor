from __future__ import annotations

import io
import csv
import json
import os
import time
import uuid
import zipfile
from pathlib import Path
from urllib.parse import urlencode, urlparse

import geopandas as gpd
import httpx
import pandas as pd

from .audit import parse_csv
from .core import Config, digest, file_hash, now, write_json

IBGE_URL = "https://geoftp.ibge.gov.br/organizacao_do_territorio/malhas_territoriais/malhas_municipais/municipio_2024/UFs/SP/SP_Municipios_2024.zip"


def validate_download(path: Path, kind: str):
    if kind == "csv":
        frame, _, _ = parse_csv(path.read_bytes())
        if len(frame.columns) < 2:
            raise ValueError("Resposta não contém tabela CSV")
    elif kind == "zip":
        with zipfile.ZipFile(path) as z:
            if z.testzip() is not None or not z.namelist():
                raise ValueError("ZIP inválido")
    elif kind == "grib":
        with path.open("rb") as f:
            if f.read(4) != b"GRIB":
                raise ValueError("Resposta não é GRIB")
        with path.open("rb") as f:
            f.seek(-4, 2)
            if f.read() != b"7777":
                raise ValueError("GRIB incompleto")
    elif kind == "tif":
        with path.open("rb") as f:
            if f.read(4) not in (b"II*\x00", b"MM\x00*", b"II+\x00", b"MM\x00+"):
                raise ValueError("Resposta não é TIFF")
    else:
        raise ValueError("Formato de download não permitido")


def download(cfg: Config, url: str, relative: str, kind: str,
             source: str, metadata: dict | None = None, public_url: str | None = None,
             immutable_revision: bool = False) -> dict:
    """Download limitado, validado e atômico. Nunca sobrescreve a Bronze."""
    if urlparse(url).scheme != "https":
        raise ValueError("Use HTTPS")
    destination = cfg.path(relative)
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and not immutable_revision:
        validate_download(destination, kind)
        return {"path": str(destination), "sha256": file_hash(destination), "status": "already_exists"}
    temporary = destination.with_name(destination.name + "." + uuid.uuid4().hex + ".part")
    for attempt in range(3):
        try:
            with httpx.Client(timeout=httpx.Timeout(60, connect=20), follow_redirects=True) as client:
                with client.stream("GET", url) as response:
                    response.raise_for_status()
                    size = 0
                    with temporary.open("wb") as output:
                        for block in response.iter_bytes():
                            size += len(block)
                            if size > cfg.max_download_mb * 1024 * 1024:
                                raise ValueError("Download excedeu o limite configurado")
                            output.write(block)
            validate_download(temporary, kind)
            sha = file_hash(temporary)
            if destination.exists():
                if file_hash(destination) == sha:
                    temporary.unlink()
                    return {"path": str(destination), "sha256": sha, "status": "unchanged"}
                destination = destination.with_name(destination.stem + "-" + sha[:12] + destination.suffix)
                if destination.exists():
                    temporary.unlink()
                    return {"path": str(destination), "sha256": sha, "status": "already_exists"}
            os.replace(temporary, destination)
            record = {"path": str(destination), "sha256": sha, "source": source,
                      "source_url": public_url or url, "collected_at": now(), "bytes": size,
                      "status": "downloaded", **(metadata or {})}
            write_json(destination.with_suffix(destination.suffix + ".metadata.json"), record)
            return record
        except (httpx.HTTPError, ValueError, zipfile.BadZipFile, csv.Error) as exc:
            temporary.unlink(missing_ok=True)
            # Exceções HTTP podem incluir a chave FIRMS na URL; não persistir o texto.
            if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code in (400, 401, 403, 404):
                raise RuntimeError(f"Fonte {source}: HTTP {exc.response.status_code}; confira acesso e período") from None
            if attempt == 2:
                raise RuntimeError(f"Fonte {source}: download/validação falhou ({type(exc).__name__})") from None
            time.sleep(2 ** attempt)
    raise RuntimeError("Download não concluído")


def collect_ibge(cfg: Config):
    return download(cfg, IBGE_URL, cfg.boundaries_relative, "zip", "ibge",
                    {"source_product": "municipal_boundaries_2024", "license_review": "documentar atribuição IBGE"})


def collect_firms(cfg: Config, start: str, days: int = 3):
    if not 1 <= days <= 5:
        raise ValueError("FIRMS permite 1 a 5 dias por requisição")
    date = pd.Timestamp(start).strftime("%Y-%m-%d")
    key = os.environ.get("FIRMS_MAP_KEY")
    if not key:
        raise ValueError("Defina FIRMS_MAP_KEY no ambiente; não grave a chave no projeto")
    allowed = {"VIIRS_SNPP_NRT", "VIIRS_SNPP_SP", "VIIRS_NOAA20_NRT", "VIIRS_NOAA20_SP", "VIIRS_NOAA21_NRT"}
    if cfg.firms_product not in allowed:
        raise ValueError("Produto VIIRS inválido")
    bbox = ",".join(str(x) for x in cfg.bbox)
    base = "https://firms.modaps.eosdis.nasa.gov/api/area/csv/"
    suffix = f"/{cfg.firms_product}/{bbox}/{days}/{date}"
    return download(cfg, base + key + suffix,
        f"data/bronze/firms/{cfg.firms_product}/{date}-{days}d.csv", "csv", "firms",
        {"source_product": cfg.firms_product, "bbox": cfg.bbox, "query_start": date, "query_days": days},
        public_url=base + "REDACTED" + suffix, immutable_revision=True)


def collect_gfs(cfg: Config, date: str, cycle: int, leads: list[int]):
    if cycle not in (0, 6, 12, 18) or any(h < 0 or h > 48 or h % 3 for h in leads):
        raise ValueError("Use ciclo 0/6/12/18 e horizontes 0–48 em passos de 3h")
    day = pd.Timestamp(date).strftime("%Y%m%d")
    issued = pd.Timestamp(f"{pd.Timestamp(date).strftime('%Y-%m-%d')} {cycle:02}:00", tz="UTC")
    records = []
    for lead in sorted(set(leads)):
        west, south, east, north = cfg.bbox
        query = {"file": f"gfs.t{cycle:02}z.pgrb2.0p25.f{lead:03}",
                 "dir": f"/gfs.{day}/{cycle:02}/atmos", "subregion": "",
                 "leftlon": west, "rightlon": east, "toplat": north, "bottomlat": south,
                 "var_TMP": "on", "var_RH": "on", "var_UGRD": "on", "var_VGRD": "on", "var_APCP": "on",
                 "lev_2_m_above_ground": "on", "lev_10_m_above_ground": "on", "lev_surface": "on"}
        url = "https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?" + urlencode(query)
        records.append(download(cfg, url,
            f"data/bronze/gfs/{day}/{cycle:02}/f{lead:03}.grib2", "grib", "gfs",
            {"source_product": "gfs_0p25", "issued_at": issued.isoformat(),
             "valid_at": (issued + pd.Timedelta(hours=lead)).isoformat(), "lead_hours": lead,
             "bbox": cfg.bbox}))
    return records


def import_inmet(cfg: Config, archive: Path):
    """Importa CSVs anuais oficiais com 8 linhas de metadados; conserva campos brutos."""
    frames, errors = [], []
    with zipfile.ZipFile(archive) as z:
        for name in z.namelist():
            if not name.lower().endswith(".csv"):
                continue
            raw = z.read(name)
            try:
                text = raw.decode("utf-8-sig")
            except UnicodeDecodeError:
                text = raw.decode("latin-1")
            lines = text.splitlines()
            header = next((i for i, line in enumerate(lines) if "DATA" in line.upper() and "HORA" in line.upper() and ";" in line), None)
            if header is None:
                errors.append({"member": name, "reason": "header_not_found"}); continue
            meta = {}
            for line in lines[:header]:
                parts = line.split(";", 1)
                if len(parts) == 2:
                    meta[parts[0].strip().rstrip(":").upper()] = parts[1].strip()
            if meta.get("UF", "").upper() != "SP":
                continue
            try:
                df = pd.read_csv(io.StringIO("\n".join(lines[header:])), sep=";", decimal=",", dtype=str)
                date_col = next(c for c in df if c.upper().startswith("DATA"))
                hour_col = next(c for c in df if "HORA" in c.upper())
                time_text = df[hour_col].str.replace(" UTC", "", regex=False).str.replace(":", "", regex=False).str.zfill(4)
                timestamps = pd.to_datetime(df[date_col].str.replace("/", "-", regex=False) + " " + time_text.str[:2] + ":" + time_text.str[2:], utc=True, errors="coerce")
                if timestamps.isna().any():
                    raise ValueError("invalid_timestamp")
                columns = {}
                selectors = {"precipitation_mm": "PRECIPITA", "temperature_c": "TEMPERATURA DO AR - BULBO SECO",
                             "relative_humidity_pct": "UMIDADE RELATIVA DO AR, HORARIA",
                             "wind_ms": "VENTO, VELOCIDADE HORARIA", "radiation_kj_m2": "RADIACAO GLOBAL"}
                from .audit import fold
                for target, match in selectors.items():
                    found = next((c for c in df if match in fold(c)), None)
                    columns[target] = pd.to_numeric(df[found].str.replace(",", ".", regex=False), errors="coerce").replace(-9999, float("nan")) if found else float("nan")
                normalized = pd.DataFrame({"observed_at": timestamps, **columns})
                normalized["station_code"] = meta.get("CODIGO (WMO)") or meta.get("CÓDIGO (WMO)")
                normalized["latitude"] = float(meta["LATITUDE"].replace(",", "."))
                normalized["longitude"] = float(meta["LONGITUDE"].replace(",", "."))
                normalized["available_at"] = pd.NaT
                normalized["available_at_known"] = False
                normalized["bronze_reference"] = str(archive) + "::" + name
                normalized["member_sha256"] = digest(raw)
                normalized["quality_flag"] = "historical_availability_unknown"
                frames.append(normalized)
            except Exception as exc:
                errors.append({"member": name, "reason": str(exc)})
    if not frames:
        raise ValueError("Nenhuma estação SP válida encontrada")
    result = pd.concat(frames, ignore_index=True)
    if result.station_code.isna().any():
        raise ValueError("Estação sem código WMO")
    valid = (result.precipitation_mm.isna() | result.precipitation_mm.ge(0)) & (result.relative_humidity_pct.isna() | result.relative_humidity_pct.between(0, 100)) & (result.wind_ms.isna() | result.wind_ms.ge(0)) & (result.temperature_c.isna() | result.temperature_c.between(-30, 60))
    base = cfg.path("data/silver/inmet/" + file_hash(archive)[:24])
    base.mkdir(parents=True, exist_ok=True)
    result.loc[valid].drop_duplicates(["station_code", "observed_at"]).to_parquet(base / "observations.parquet", index=False)
    result.loc[~valid].to_parquet(base / "exceptions.parquet", index=False)
    write_json(base / "manifest.json", {"archive": str(archive), "sha256": file_hash(archive),
        "rows": len(result), "accepted": int(valid.sum()), "member_errors": errors, "imported_at": now()})
    return {"path": str(base), "rows": len(result), "member_errors": errors}


def import_landcover(cfg: Config, path: Path, collection: str, published_at: str):
    """Contrato tabular explícito; não soma classes hierárquicas sobrepostas."""
    df = pd.read_csv(path, dtype={"municipality_code": str, "class_code": str})
    required = {"municipality_code", "year", "class_code", "area_ha"}
    if not required <= set(df):
        raise ValueError("CSV deve conter municipality_code,year,class_code,area_ha (classes disjuntas)")
    df = df[df.municipality_code.str.startswith(cfg.state_code)].copy()
    if df.empty or df.duplicated(["municipality_code", "year", "class_code"]).any():
        raise ValueError("Dados vazios ou classes duplicadas")
    df["area_ha"] = pd.to_numeric(df.area_ha, errors="raise")
    if df.area_ha.isna().any() or df.area_ha.lt(0).any():
        raise ValueError("Área inválida")
    denominator = df.groupby(["municipality_code", "year"]).area_ha.transform("sum")
    if denominator.le(0).any():
        raise ValueError("Área total deve ser positiva")
    df["fraction"] = df.area_ha / denominator
    df["collection"] = collection
    df["available_at"] = pd.Timestamp(published_at, tz="UTC") if pd.Timestamp(published_at).tzinfo is None else pd.Timestamp(published_at).tz_convert("UTC")
    df["bronze_sha256"] = file_hash(path)
    base = cfg.path("data/silver/mapbiomas/" + digest((file_hash(path) + collection + published_at).encode())[:24])
    base.mkdir(parents=True, exist_ok=True)
    df.to_parquet(base / "landcover.parquet", index=False)
    return {"path": str(base), "rows": len(df)}


def gfs_points(cfg: Config, archive: Path):
    """Extrai grade sem interpolação; chuva acumulada permanece por intervalo GRIB."""
    import xarray as xr
    import cfgrib
    sidecar = archive.with_suffix(archive.suffix + ".metadata.json")
    if not sidecar.exists():
        raise ValueError("GFS sem metadados de emissão e coleta")
    metadata = json.loads(sidecar.read_text(encoding="utf-8"))
    datasets = cfgrib.open_datasets(str(archive), backend_kwargs={"indexpath": "", "read_keys": ["stepRange", "startStep", "endStep"]})
    records = []
    try:
        for ds in datasets:
            for name, array in ds.data_vars.items():
                if name not in {"t2m", "r2", "u10", "v10", "tp"}:
                    continue
                tab = array.to_dataframe(name="value").reset_index()
                tab = tab[["latitude", "longitude", "value"]]
                tab["longitude"] = ((tab.longitude + 180) % 360) - 180
                tab["variable"] = name
                tab["units"] = array.attrs.get("units")
                tab["step_type"] = array.attrs.get("GRIB_stepType")
                tab["step_range"] = array.attrs.get("GRIB_stepRange")
                tab["issued_at"] = pd.Timestamp(metadata["issued_at"])
                tab["valid_at"] = pd.Timestamp(metadata["valid_at"])
                tab["available_at"] = pd.Timestamp(metadata["collected_at"])
                tab["bronze_sha256"] = metadata["sha256"]
                records.append(tab)
    finally:
        for ds in datasets:
            ds.close()
    if not records:
        raise ValueError("GRIB sem variáveis esperadas")
    result = pd.concat(records, ignore_index=True)
    dest = cfg.path("data/silver/gfs/" + file_hash(archive)[:24] + ".parquet")
    dest.parent.mkdir(parents=True, exist_ok=True)
    result.to_parquet(dest, index=False)
    return {"path": str(dest), "rows": len(result)}
