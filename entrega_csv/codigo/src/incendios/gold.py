from __future__ import annotations

import json
import uuid
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .core import Config, VERSION, current_dir, digest, file_hash, now, publish_dir, read_table, write_json

COVERAGE_COLUMNS = {"municipality_code", "product", "start_utc", "end_utc",
                    "observation_complete", "provenance_verified", "timezone_verified", "evidence"}
FEATURE_COLUMNS = {"municipality_code", "feature", "value", "valid_from", "valid_to", "available_at", "source", "unit"}


def read_coverage(cfg: Config) -> pd.DataFrame:
    path = cfg.path("metadata/coverage_approvals.csv")
    if not path.exists():
        return pd.DataFrame(columns=sorted(COVERAGE_COLUMNS))
    data = pd.read_csv(path, dtype={"municipality_code": str}, keep_default_na=False)
    if not COVERAGE_COLUMNS <= set(data):
        raise ValueError("Manifesto de cobertura sem campos obrigatórios")
    for c in ("observation_complete", "provenance_verified", "timezone_verified"):
        if not data[c].astype(str).str.lower().isin(["true", "false"]).all():
            raise ValueError(f"{c} exige true/false explícito")
        data[c] = data[c].astype(str).str.lower().eq("true")
    for c in ("start_utc", "end_utc"):
        data[c] = pd.to_datetime(data[c], utc=True, errors="raise")
    if data.start_utc.ge(data.end_utc).any():
        raise ValueError("Intervalo de cobertura inválido")
    if (data.observation_complete & data.evidence.str.strip().eq("")).any():
        raise ValueError("Cobertura aprovada exige referência à evidência")
    return data


def coverage_mask(coverage: pd.DataFrame, code: str, product: str, starts, ends) -> np.ndarray:
    """Intervalos [start,end) aprovados; só libera janelas integralmente cobertas."""
    result = np.zeros(len(starts), dtype=bool)
    if coverage.empty:
        return result
    selected = coverage[(coverage.municipality_code.isin([code, "*"])) & coverage["product"].eq(product)
                        & coverage.observation_complete & coverage.provenance_verified & coverage.timezone_verified]
    # Une intervalos adjacentes para não perder janelas que cruzam arquivos anuais.
    merged = []
    for start, end in selected.sort_values("start_utc")[["start_utc", "end_utc"]].itertuples(index=False, name=None):
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], end)
        else:
            merged.append([start, end])
    for start, end in merged:
        result |= np.asarray((starts >= start) & (ends <= end))
    return result


def import_features(cfg: Config, path: Path) -> dict:
    data = pd.read_csv(path, dtype={"municipality_code": str})
    if not FEATURE_COLUMNS <= set(data):
        raise ValueError("Features exigem municipality_code,feature,value,valid_from,valid_to,available_at,source,unit")
    for c in ("valid_from", "valid_to", "available_at"):
        data[c] = pd.to_datetime(data[c], utc=True, errors="coerce")
    data["value"] = pd.to_numeric(data.value, errors="raise")
    if np.isinf(data.value).any():
        raise ValueError("Feature com valor infinito")
    if data[["valid_from", "valid_to"]].isna().any().any() or data.valid_from.ge(data.valid_to).any():
        raise ValueError("Validade das features inválida")
    if not data.feature.str.fullmatch(r"[a-z][a-z0-9_]*").all():
        raise ValueError("Nome de feature inválido")
    reserved = {"target", "issuance_at", "target_end", "product", "municipality_code", "split", "coverage_known", "track"}
    if set(data.feature) & reserved:
        raise ValueError("Nome de feature reservado")
    data["bronze_sha256"] = file_hash(path)
    identifier = file_hash(path)[:24]
    dest = cfg.path(f"data/silver/features/{identifier}.parquet")
    dest.parent.mkdir(parents=True, exist_ok=True)
    data.to_parquet(dest, index=False)
    return {"path": str(dest), "rows": len(data)}


def attach_features(table: pd.DataFrame, features: pd.DataFrame, track: str) -> pd.DataFrame:
    """Junção ponto-no-tempo: validade e disponibilidade anterior à emissão."""
    if features.empty:
        return table
    result = table.copy()
    for name in sorted(features.feature.unique()):
        result[name] = np.nan
        result[name + "_age_hours"] = np.nan
        result[name + "_missing"] = True
        for code in result.municipality_code.unique():
            indices = result.index[result.municipality_code.eq(code)]
            times = result.loc[indices, "issuance_at"]
            candidates = features[features.feature.eq(name) & features.municipality_code.eq(code)].copy()
            if track == "operational":
                candidates = candidates[candidates.available_at.notna()]
            candidates = candidates.sort_values(["available_at", "valid_from"], na_position="first")
            for row in candidates.itertuples():
                mask = times.ge(row.valid_from) & times.lt(row.valid_to)
                if track == "operational":
                    mask &= times.ge(row.available_at)
                chosen = indices[mask]
                result.loc[chosen, name] = row.value
                result.loc[chosen, name + "_missing"] = pd.isna(row.value)
                if pd.notna(row.available_at):
                    result.loc[chosen, name + "_age_hours"] = (result.loc[chosen, "issuance_at"] - row.available_at).dt.total_seconds() / 3600
    return result


def build_gold(cfg: Config, start: str, end: str, product: str = "reference_history",
               codes: list[str] | None = None, track: str = "retrospective",
               target_mode: str = "verified_observation") -> dict:
    if track not in ("retrospective", "operational"):
        raise ValueError("Trilha inválida")
    if target_mode not in ("verified_observation", "published_archive"):
        raise ValueError("Modo de alvo inválido")
    if target_mode == "published_archive" and track != "retrospective":
        raise ValueError("Arquivo publicado só permite estudo retrospectivo")
    observations = read_table(cfg, "observations")
    municipalities = read_table(cfg, "municipalities")
    if municipalities.empty:
        raise ValueError("Execute auditoria com a malha IBGE antes de construir Ouro")
    if observations.empty or product not in set(observations["source_product"]):
        raise ValueError("Produto não encontrado na Prata")
    selected = observations[observations.source_product.eq(product)].copy()
    selected["observed_at"] = pd.to_datetime(selected.observed_at, utc=True)
    selected["available_at"] = pd.to_datetime(selected.available_at, utc=True, errors="coerce")
    if codes is not None:
        unknown = set(codes) - set(municipalities.municipality_code)
        if unknown:
            raise ValueError(f"Municípios desconhecidos: {sorted(unknown)}")
        municipalities = municipalities[municipalities.municipality_code.isin(codes)]
    local_dates = pd.date_range(start, end, freq="D")
    if len(local_dates) == 0:
        raise ValueError("Período vazio")
    issues = (local_dates + pd.Timedelta(hours=cfg.issuance_hour)).tz_localize(cfg.timezone).tz_convert("UTC")
    ends = issues + pd.Timedelta(hours=24)
    coverage = read_coverage(cfg)
    archive_known, archive_info = None, None
    archive_history = {}
    if target_mode == "published_archive":
        from .archive_proxy import archive_mask
        archive_known, archive_info = archive_mask(cfg, product, issues, ends)
        for days in (1, 7):
            archive_history[days], _ = archive_mask(cfg, product, issues - pd.Timedelta(days=days), issues)
    environment_paths = sorted(cfg.path("data/silver/environment_daily").glob("year=*/part.parquet"))
    environment = {}
    if environment_paths and track == "retrospective":
        for path in environment_paths:
            year = int(path.parent.name.split("=")[1])
            if year in set(local_dates.year):
                metadata = json.loads((path.parent / "manifest.json").read_text(encoding="utf-8"))
                if metadata["sha256"] != file_hash(path):
                    raise ValueError("Variáveis ambientais alteradas")
                environment[year] = pd.read_parquet(path).set_index(["municipality_code", "issuance_at"])
        if environment:
            schemas = [list(frame.columns) for frame in environment.values()]
            if any(s != schemas[0] for s in schemas):
                raise ValueError("Esquema ambiental varia entre anos")
    feature_paths = sorted(cfg.path("data/silver/features").glob("*.parquet"))
    derived_index = cfg.path("metadata/derived_features_active.json")
    if derived_index.exists():
        for value in json.loads(derived_index.read_text(encoding="utf-8")).values():
            derived_path = cfg.path(value)
            if not derived_path.is_relative_to(cfg.root):
                raise ValueError("Feature derivada fora do Data Lake")
            feature_paths.append(derived_path)
    features = pd.concat([pd.read_parquet(p) for p in feature_paths], ignore_index=True) if feature_paths else pd.DataFrame()
    if not features.empty:
        # Revisões sem seleção explícita não podem depender da ordem dos arquivos.
        key = ["municipality_code", "feature", "valid_from", "valid_to", "available_at"]
        if features.duplicated(key, keep=False).any():
            raise ValueError("Features com revisões ambíguas: selecione/remova a publicação antiga de silver/features")
    identity = {"silver_snapshot": current_dir(cfg, "silver").name, "start": start, "end": end,
                "product": product, "codes": municipalities.municipality_code.tolist(), "track": track,
                "coverage_sha256": file_hash(cfg.path("metadata/coverage_approvals.csv")) if not coverage.empty else None,
                "features": [file_hash(p) for p in feature_paths], "version": VERSION,
                "target_mode": target_mode, "archive_manifest": archive_info,
                "environment_features": [file_hash(p) for p in environment_paths] if track == "retrospective" else [],
                "implementation": file_hash(Path(__file__)), "config": cfg.__dict__}
    identifier = digest(json.dumps(identity, sort_keys=True).encode())[:24]
    staging = cfg.path(f"data/gold/.staging-{uuid.uuid4().hex}")
    staging.mkdir(parents=True)
    output = staging / "municipality_day"
    output.mkdir()
    total, labeled, positives = 0, 0, 0
    writers = {}
    for code, name in municipalities[["municipality_code", "municipality_name"]].itertuples(index=False, name=None):
        local = selected[selected.municipality_code.eq(code)].sort_values("observed_at")
        detection_times = pd.DatetimeIndex(local.observed_at)
        counts = detection_times.searchsorted(ends, side="left") - detection_times.searchsorted(issues, side="left")
        covered = coverage_mask(coverage, code, product, issues, ends)
        labeled_window = archive_known if target_mode == "published_archive" else covered
        target = pd.array(np.where(counts > 0, 1, 0), dtype="Int8")
        target[~labeled_window] = pd.NA
        data = pd.DataFrame({"municipality_code": code, "municipality_name": name,
            "issuance_at": issues, "target_end": ends, "product": product, "track": track,
            "target": target, "coverage_known": covered,
            "target_mode": target_mode, "archive_known": archive_known if archive_known is not None else False,
            "observed_target_detections": counts,
            "month_sin": np.sin(2 * np.pi * local_dates.month / 12),
            "month_cos": np.cos(2 * np.pi * local_dates.month / 12)})
        for days in (1, 7):
            past_start = issues - pd.Timedelta(days=days)
            history_complete = coverage_mask(coverage, code, product, past_start, issues)
            if target_mode == "published_archive":
                history_complete = archive_history[days]
            if track == "retrospective":
                historical = detection_times.searchsorted(issues, side="left") - detection_times.searchsorted(past_start, side="left")
            else:
                historical = []
                for left, right in zip(past_start, issues):
                    historical.append(int((local.observed_at.ge(left) & local.observed_at.lt(right)
                                           & local.available_at.notna() & local.available_at.le(right)).sum()))
                # Histórico sem disponibilidade conhecida não gera uma contagem operacional.
                history_complete &= np.array([not ((local.observed_at.ge(left) & local.observed_at.lt(right)) & local.available_at.isna()).any() for left, right in zip(past_start, issues)])
            data[f"focos_previous_{days}d"] = np.where(history_complete, historical, np.nan)
            data[f"focos_previous_{days}d_missing"] = ~history_complete
        data = attach_features(data, features, track)
        if environment:
            pieces = []
            for year, frame in environment.items():
                if code in frame.index.get_level_values(0):
                    pieces.append(frame.xs(code, level=0))
            if pieces:
                extra = pd.concat(pieces)
                data = data.join(extra, on="issuance_at", validate="one_to_one")
        years = data.issuance_at.dt.tz_convert(cfg.timezone).dt.year
        data["split"] = np.select([years.le(2021), years.le(2023), years.le(2025)], ["train", "validation", "test"], default="prospective")
        for year in sorted(years.unique()):
            directory = output / f"year={year}"
            directory.mkdir(exist_ok=True)
            arrow = pa.Table.from_pandas(data.loc[years.eq(year)], preserve_index=False)
            if year not in writers:
                writers[year] = pq.ParquetWriter(directory / "part-00000.parquet", arrow.schema, compression="zstd")
            writers[year].write_table(arrow.cast(writers[year].schema))
        total += len(data); labeled += int(data.target.notna().sum()); positives += int(data.target.eq(1).sum())
    for writer in writers.values():
        writer.close()
    manifest = {"snapshot_id": identifier, "created_at": now(), **identity,
                "rows": total, "labeled_rows": labeled, "positive_rows": positives,
                "operational_claim_allowed": track == "operational" and target_mode == "verified_observation",
                "target_definition": "ao menos uma detecção do produto no município em [emissão, emissão+24h)",
                "unknown_coverage_is_negative": False,
                "archive_proxy_labels": target_mode == "published_archive",
                "archive_proxy_disclaimer": archive_info["disclaimer"] if archive_info else None}
    write_json(staging / "manifest.json", manifest)
    dest = publish_dir(cfg, "gold", staging, identifier)
    return {"path": str(dest), "snapshot_id": identifier, "rows": total, "labeled_rows": labeled,
            "positive_rows": positives, "municipalities": len(municipalities), "product": product, "track": track}


def read_gold(cfg: Config, splits: tuple[str, ...] | None = None) -> tuple[pd.DataFrame, dict]:
    directory = current_dir(cfg, "gold")
    if directory is None:
        raise ValueError("Base Ouro não encontrada")
    files = sorted((directory / "municipality_day").rglob("*.parquet"))
    frames = []
    for path in files:
        frame = pd.read_parquet(path, filters=[("split", "in", list(splits))] if splits else None)
        if len(frame):
            frames.append(frame)
    if not frames:
        raise ValueError("Sem linhas Ouro para as divisões pedidas")
    return pd.concat(frames, ignore_index=True), json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
