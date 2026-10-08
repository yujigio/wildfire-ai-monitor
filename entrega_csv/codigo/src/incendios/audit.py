from __future__ import annotations

import calendar
import csv
import io
import json
import re
import unicodedata
import uuid
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd

from .core import Config, VERSION, digest, file_hash, now, publish_dir, write_json

ANNUAL = {"id_bdq", "foco_id", "lat", "lon", "data_pas", "municipio", "estado"}
MONTHLY = {"id", "lat", "lon", "data_hora_gmt", "satelite", "municipio", "estado"}
FIRMS = {"latitude", "longitude", "acq_date", "acq_time", "satellite"}


def fold(value: str) -> str:
    value = unicodedata.normalize("NFKD", str(value))
    return "".join(c for c in value if not unicodedata.combining(c)).upper().strip()


def parse_csv(raw: bytes) -> tuple[pd.DataFrame, str, str]:
    if b"\x00" in raw:
        raise ValueError("CSV com bytes nulos")
    try:
        text = raw.decode("utf-8-sig")
        encoding = "utf-8-sig"
    except UnicodeDecodeError:
        text = raw.decode("latin-1")
        encoding = "latin-1"
    dialect = csv.Sniffer().sniff(text[:16000], delimiters=",;\t")
    reader = csv.DictReader(io.StringIO(text), dialect=dialect)
    columns = reader.fieldnames
    if not columns or len(columns) != len(set(columns)):
        raise ValueError("Cabeçalho vazio ou duplicado")
    rows = []
    for row in reader:
        if None in row or any(v is None for v in row.values()):
            raise ValueError("Linha com número incorreto de colunas")
        rows.append(row)
    return pd.DataFrame(rows, columns=columns), encoding, dialect.delimiter


def product_for(columns) -> tuple[str, str]:
    columns = set(columns)
    if ANNUAL <= columns:
        return "inpe", "reference_history"
    if MONTHLY <= columns:
        return "inpe", "multisatellite_monitoring"
    if FIRMS <= columns:
        return "firms", "viirs_monitoring"
    raise ValueError("Esquema não reconhecido; revisão necessária")


def load_boundaries(cfg: Config) -> gpd.GeoDataFrame | None:
    path = cfg.path(cfg.boundaries_relative)
    if not path.exists():
        return None
    b = gpd.read_file(path)
    if b.crs is None:
        raise ValueError("Malha territorial sem CRS declarado")
    if not b.geometry.is_valid.all():
        raise ValueError("Malha territorial contém geometria inválida")
    b = b.rename(columns={cfg.boundary_code_column: "municipality_code",
                          cfg.boundary_name_column: "municipality_name"})
    if not {"municipality_code", "municipality_name"} <= set(b.columns):
        raise ValueError("Malha sem os campos de município configurados")
    b["municipality_code"] = b.municipality_code.astype(str)
    b = b[b.municipality_code.str.startswith(cfg.state_code)].copy()
    if b.empty or b.municipality_code.duplicated().any():
        raise ValueError("Malha vazia ou com municípios duplicados")
    return b[["municipality_code", "municipality_name", "geometry"]].to_crs(4326)


def normalize(df: pd.DataFrame, item: dict, cfg: Config) -> pd.DataFrame:
    source, product = product_for(df.columns)
    if product == "reference_history":
        original_id = df.foco_id.where(df.foco_id.ne(""), df.id_bdq)
        time_original = df.data_pas
        observed = pd.to_datetime(time_original, errors="coerce")
        observed = observed.dt.tz_localize(cfg.annual_source_timezone, ambiguous="NaT", nonexistent="NaT").dt.tz_convert("UTC")
        sensor = pd.Series([None] * len(df), index=df.index, dtype="object")
    elif source == "firms":
        sensor = df.satellite
        time_original = df.acq_date + " " + df.acq_time.str.zfill(4).str[:2] + ":" + df.acq_time.str.zfill(4).str[2:]
        observed = pd.to_datetime(time_original, utc=True, errors="coerce")
        original_id = pd.Series([None] * len(df), index=df.index, dtype="object")
    else:
        original_id, time_original, sensor = df.id, df.data_hora_gmt, df.satelite
        observed = pd.to_datetime(time_original, utc=True, errors="coerce")
    lat = pd.to_numeric(df["latitude" if source == "firms" else "lat"], errors="coerce")
    lon = pd.to_numeric(df["longitude" if source == "firms" else "lon"], errors="coerce")
    rows = pd.DataFrame({
        "original_id": original_id, "source": source, "product": product,
        "sensor": sensor, "observed_at": observed, "time_original": time_original,
        "latitude": lat, "longitude": lon, "source_crs": "EPSG:4326_assumed",
        "municipality_original": df.get("municipio", pd.Series("", index=df.index)),
        "state_original": df.get("estado", pd.Series("", index=df.index)),
        "municipality_code_original": df.get("municipio_id", pd.Series("", index=df.index)),
        "bronze_reference": item["reference"], "bronze_sha256": item["content_sha256"],
        "source_row": range(2, len(df) + 2), "transform_version": VERSION,
        "collected_at": item.get("collected_at"), "available_at": item.get("collected_at"),
        "available_at_known": bool(item.get("collected_at")),
    })
    if item.get("source_product"):
        rows["source_product"] = item["source_product"]
    else:
        rows["source_product"] = product
    raw_json = [json.dumps(r, ensure_ascii=False, sort_keys=True) for r in df.to_dict("records")]
    rows["original_attributes"] = raw_json
    rows["content_fingerprint"] = [digest(s.encode()) for s in raw_json]
    # Deduplicação por identidade exata de observação; não agrupa focos vizinhos.
    rows["observation_id"] = [digest(json.dumps([source, product, item.get("source_product", product),
        None if pd.isna(s) else str(s), None if pd.isna(t) else str(t),
        None if pd.isna(a) else float(a), None if pd.isna(o) else float(o)],
        ensure_ascii=False).encode()) for s, t, a, o in zip(sensor, observed, lat, lon)]
    reasons = [[] for _ in range(len(df))]
    flags = [[] for _ in range(len(df))]
    expected = item.get("expected_period")
    for i, row in enumerate(rows.itertuples()):
        if pd.isna(row.observed_at):
            reasons[i].append("invalid_timestamp")
        elif expected and row.observed_at.strftime("%Y-%m").replace("-", "").startswith(expected) is False:
            reasons[i].append("outside_filename_period")
        if pd.isna(row.latitude) or pd.isna(row.longitude) or not (-90 <= row.latitude <= 90 and -180 <= row.longitude <= 180):
            reasons[i].append("invalid_coordinates")
        if not row.original_id and source == "inpe":
            flags[i].append("missing_original_id")
        if product == "reference_history":
            flags[i].append("sensor_provenance_unverified")
            if not cfg.annual_timezone_verified:
                flags[i].append("timezone_unverified")
        flags[i].append("source_crs_assumed")
        if not item.get("collected_at"):
            flags[i].append("historical_availability_unknown")
    rows["exclusion_reasons"] = reasons
    rows["quality_flags"] = flags
    return rows


def assign_territory(rows: pd.DataFrame, boundaries: gpd.GeoDataFrame | None) -> pd.DataFrame:
    rows = rows.copy()
    rows["municipality_code"] = pd.Series(pd.NA, index=rows.index, dtype="string")
    rows["municipality_name"] = pd.Series(pd.NA, index=rows.index, dtype="string")
    if boundaries is None:
        rows["quality_flags"] = rows.quality_flags.map(lambda f: f + ["territory_unverified"])
        # Mantém em Prata apenas estados declarados SP, sem atribuir código oficial.
        for idx in rows.index:
            if fold(rows.at[idx, "state_original"]) not in {"SAO PAULO", "SP"}:
                rows.at[idx, "exclusion_reasons"] += ["declared_outside_sp_or_unknown"]
        return rows
    valid = rows.latitude.between(-90, 90) & rows.longitude.between(-180, 180)
    points = gpd.GeoDataFrame(rows.loc[valid, ["latitude", "longitude"]],
                             geometry=gpd.points_from_xy(rows.loc[valid, "longitude"], rows.loc[valid, "latitude"]), crs=4326)
    joined = gpd.sjoin(points, boundaries, how="left", predicate="intersects")
    counts = joined.groupby(level=0).municipality_code.count()
    joined = joined[~joined.index.duplicated(keep="first")]
    for idx in rows.index:
        if not valid.at[idx]:
            continue
        if counts.get(idx, 0) == 0:
            rows.at[idx, "exclusion_reasons"] += ["coordinates_outside_sp"]
        elif counts.get(idx, 0) > 1:
            rows.at[idx, "exclusion_reasons"] += ["ambiguous_municipality_boundary"]
        else:
            code = joined.at[idx, "municipality_code"]
            name = joined.at[idx, "municipality_name"]
            rows.at[idx, "municipality_code"] = code
            rows.at[idx, "municipality_name"] = name
            if rows.at[idx, "municipality_original"] and fold(name) != fold(rows.at[idx, "municipality_original"]):
                rows.at[idx, "quality_flags"] += ["municipality_mismatch"]
            if rows.at[idx, "state_original"] and fold(rows.at[idx, "state_original"]) not in {"SAO PAULO", "SP"}:
                rows.at[idx, "quality_flags"] += ["state_mismatch"]
    return rows


def audit(cfg: Config) -> dict:
    root = cfg.path(cfg.input_relative)
    if not root.is_dir():
        raise FileNotFoundError(f"Pasta de entrada não encontrada: {root}")
    files = sorted(root.glob("*.csv")) + sorted(root.glob("*.zip"))
    firms_root = cfg.path("data/bronze/firms")
    if firms_root.exists():
        files += sorted(firms_root.rglob("*.csv"))
    if not files:
        raise ValueError("Nenhum CSV/ZIP encontrado")
    csv_names = {p.name for p in files if p.suffix == ".csv"}
    inventory, frames, content_seen = [], [], set()
    provenance_path = cfg.path("metadata/source_provenance.json")
    provenance = json.loads(provenance_path.read_text(encoding="utf-8")) if provenance_path.exists() else {}
    for path in files:
        physical_hash = file_hash(path)
        try:
            if path.suffix == ".zip":
                with zipfile.ZipFile(path) as z:
                    members = [(name, z.read(name)) for name in sorted(z.namelist()) if name.lower().endswith(".csv")]
                if not members:
                    raise ValueError("ZIP sem CSV")
            else:
                members = [(None, path.read_bytes())]
        except Exception as exc:
            inventory.append({"reference": str(path), "file_sha256": physical_hash,
                              "status": "quarantined", "error": str(exc)})
            continue
        for member, raw in members:
            reference = path.relative_to(cfg.root).as_posix() + ("::" + member if member else "")
            item = {"reference": reference, "file_sha256": physical_hash, "content_sha256": digest(raw),
                    "file_size": path.stat().st_size, "content_size": len(raw),
                    "filesystem_mtime_not_collection_time": datetime.fromtimestamp(path.stat().st_mtime, timezone.utc).isoformat(),
                    "collected_at": None, "provenance_status": "unknown", "status": "inspected"}
            item.update(provenance.get(reference, {}))
            sidecar = path.with_suffix(path.suffix + ".metadata.json")
            if sidecar.exists():
                metadata = json.loads(sidecar.read_text(encoding="utf-8"))
                item.update({k: metadata[k] for k in ("collected_at", "source_url", "source_product") if k in metadata})
            base_name = Path(member).name if member else path.name
            period = re.search(r"(?:ref_|mensal_sp_)(\d{4}(?:\d{2})?)", base_name)
            item["expected_period"] = period.group(1) if period else None
            try:
                frame, encoding, separator = parse_csv(raw)
                source, product = product_for(frame.columns)
                item.update({"encoding": encoding, "delimiter": separator, "columns": list(frame.columns),
                             "rows": len(frame), "source": source, "product": product,
                             "states_declared": dict(Counter(frame.get("estado", [])))})
                normalized = normalize(frame, item, cfg)
                valid_times = normalized.observed_at.dropna()
                item["min_observed_at"] = str(valid_times.min()) if len(valid_times) else None
                item["max_observed_at"] = str(valid_times.max()) if len(valid_times) else None
                if period:
                    year = int(period.group(1)[:4]); month = int(period.group(1)[4:] or "12")
                    end = pd.Timestamp(year, month, calendar.monthrange(year, month)[1], tz="UTC") + pd.Timedelta(days=1)
                    item["period_status"] = "open_period" if end > pd.Timestamp.now(tz="UTC") else "coverage_not_verified"
                if member and base_name in csv_names:
                    csv_path = next(p for p in files if p.name == base_name)
                    same = file_hash(csv_path) == item["content_sha256"]
                    item["status"] = "duplicate_zip_copy" if same else "zip_csv_divergence"
                    item["canonical"] = csv_path.relative_to(cfg.root).as_posix()
                elif re.fullmatch(r"focos_br_ref_(\d{4})\.csv", base_name) and f"focos_br_sp_ref_{period.group(1)}.csv" in csv_names:
                    item["status"] = "national_archive_not_ingested"
                elif item["content_sha256"] in content_seen:
                    item["status"] = "duplicate_content"
                else:
                    frames.append(normalized)
                    content_seen.add(item["content_sha256"])
            except Exception as exc:
                item["status"] = "quarantined"
                item["error"] = str(exc)
            inventory.append(item)
    if not frames:
        write_json(cfg.path("metadata/quarantine/audit_inventory.json"), inventory)
        raise ValueError("Nenhum arquivo canônico válido; consulte inventário de quarentena")
    rows = pd.concat(frames, ignore_index=True)
    boundaries = load_boundaries(cfg)
    rows = assign_territory(rows, boundaries)
    duplicates = rows.observation_id.duplicated(keep="first")
    # Conflitos de identificador ficam visíveis e não são descartados como duplicação.
    ids = rows[rows.original_id.notna() & rows.original_id.ne("")]
    conflicts = ids.groupby(["source", "product", "original_id"]).observation_id.nunique()
    conflict_keys = set(conflicts[conflicts > 1].index)
    for idx in rows.index:
        if duplicates.at[idx]:
            rows.at[idx, "exclusion_reasons"] += ["duplicate_observation"]
        if (rows.at[idx, "source"], rows.at[idx, "product"], rows.at[idx, "original_id"]) in conflict_keys:
            rows.at[idx, "quality_flags"] += ["original_id_conflict"]
    accepted = rows.exclusion_reasons.map(len).eq(0)
    reasons = Counter(reason for values in rows.exclusion_reasons for reason in values)
    report = {"generated_at": now(), "version": VERSION, "rows_read": len(rows),
              "accepted": int(accepted.sum()), "excluded": int((~accepted).sum()),
              "reasons": dict(reasons), "spatial_verified": boundaries is not None,
              "files_quarantined": sum(i["status"] == "quarantined" for i in inventory),
              "zip_csv_divergences": sum(i["status"] == "zip_csv_divergence" for i in inventory),
              "labels_ready": False,
              "note": "Contagens são observações, não incêndios independentes. Cobertura e procedência requerem aprovação."}
    identity = {"inputs": [(i["reference"], i.get("content_sha256"), i["status"]) for i in inventory],
                "boundaries": file_hash(cfg.path(cfg.boundaries_relative)) if boundaries is not None else None,
                "config": cfg.__dict__, "provenance": provenance, "code": VERSION,
                "implementation": file_hash(Path(__file__))}
    snapshot_id = digest(json.dumps(identity, sort_keys=True).encode())[:24]
    staging = cfg.path(f"data/silver/.staging-{uuid.uuid4().hex}")
    staging.mkdir(parents=True)
    write_json(staging / "inventory.json", inventory)
    write_json(staging / "audit.json", report)
    write_json(staging / "manifest.json", {"snapshot_id": snapshot_id, **identity})
    rows.loc[accepted].to_parquet(staging / "observations.parquet", index=False)
    rows.loc[~accepted].to_parquet(staging / "exceptions.parquet", index=False)
    coverage = rows.loc[accepted, ["source", "product", "source_product", "sensor", "observed_at", "municipality_code", "municipality_original"]].copy()
    coverage["month"] = coverage.observed_at.dt.strftime("%Y-%m")
    counts = coverage.groupby(["source", "product", "source_product", "sensor", "month", "municipality_code", "municipality_original"], dropna=False).size().reset_index(name="observations")
    counts["coverage_status"] = "not_verified"
    counts.to_parquet(staging / "coverage_counts.parquet", index=False)
    if boundaries is not None:
        boundaries.to_parquet(staging / "territories.parquet", index=False)
        boundaries.drop(columns="geometry").to_parquet(staging / "municipalities.parquet", index=False)
        boundaries.to_file(staging / "territories.geojson", driver="GeoJSON")
    dest = publish_dir(cfg, "silver", staging, snapshot_id)
    report["snapshot_path"] = str(dest)
    write_json(cfg.path("metadata/audit_latest.json"), report)
    return report
