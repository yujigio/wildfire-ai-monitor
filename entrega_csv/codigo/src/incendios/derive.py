from __future__ import annotations

import json
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from .core import Config, digest, file_hash, now, read_table, write_json


def derive_features(cfg: Config, start: str, end: str, codes: list[str] | None = None):
    """Produz features com validade e disponibilidade explícitas, sem inventar cobertura."""
    dates = pd.date_range(start, end, freq="D")
    if not 0 < len(dates) <= 370:
        raise ValueError("Processe lotes de até 370 dias")
    silver = read_table(cfg, "municipalities")
    if silver.empty:
        raise ValueError("Execute auditoria com IBGE")
    codes = cfg.pilot_codes if codes is None else codes
    if set(codes) - set(silver.municipality_code):
        raise ValueError("Município não encontrado")
    from .core import current_dir
    boundaries = gpd.read_parquet(current_dir(cfg, "silver") / "territories.parquet")
    boundaries = boundaries[boundaries.municipality_code.isin(codes)].to_crs(5880)
    centers = boundaries.geometry.representative_point()
    issue_times = (dates + pd.Timedelta(hours=cfg.issuance_hour)).tz_localize(cfg.timezone).tz_convert("UTC")
    inmet_files = sorted(cfg.path("data/silver/inmet").glob("*/observations.parquet"))
    met = pd.concat([pd.read_parquet(p) for p in inmet_files], ignore_index=True) if inmet_files else pd.DataFrame()
    land_files = sorted(cfg.path("data/silver/mapbiomas").glob("*/landcover.parquet"))
    land = pd.concat([pd.read_parquet(p) for p in land_files], ignore_index=True) if land_files else pd.DataFrame()
    gfs_files = sorted(cfg.path("data/silver/gfs").glob("*.parquet"))
    gfs = pd.concat([pd.read_parquet(p) for p in gfs_files], ignore_index=True) if gfs_files else pd.DataFrame()
    stations = None
    if not met.empty:
        met.observed_at = pd.to_datetime(met.observed_at, utc=True)
        met.available_at = pd.to_datetime(met.available_at, utc=True, errors="coerce")
        duplicate = met.duplicated(["station_code", "observed_at"], keep=False)
        if duplicate.any():
            comparison = met.loc[duplicate].groupby(["station_code", "observed_at"])[["temperature_c", "precipitation_mm", "relative_humidity_pct", "wind_ms"]].nunique(dropna=False)
            if comparison.gt(1).any().any():
                raise ValueError("INMET com revisões conflitantes: revisar antes de derivar")
            met = met.drop_duplicates(["station_code", "observed_at"])
        station_table = met[["station_code", "latitude", "longitude"]].drop_duplicates()
        if station_table.station_code.duplicated().any():
            raise ValueError("Estação deslocada no histórico: particionar por localização")
        stations = gpd.GeoDataFrame(station_table,
            geometry=gpd.points_from_xy(station_table.longitude, station_table.latitude), crs=4326).to_crs(5880)
    rows, selection = [], []
    def add(code, name, value, issue, available, source, unit, reference):
        rows.append({"municipality_code": code, "feature": name, "value": value,
                     "valid_from": issue, "valid_to": issue + pd.Timedelta(hours=24),
                     "available_at": available, "source": source, "unit": unit,
                     "reference": reference})
    for idx, boundary in boundaries.iterrows():
        code = boundary.municipality_code
        station_data = pd.DataFrame()
        if stations is not None:
            distances = stations.geometry.distance(centers.loc[idx])
            closest = distances.idxmin()
            if distances.loc[closest] <= 60000:
                station_code = stations.loc[closest, "station_code"]
                station_data = met[met.station_code.eq(station_code)]
                selection.append({"municipality_code": code, "station_code": station_code,
                                  "distance_m": float(distances.loc[closest]), "method": "representative_point_nearest_60km"})
        for issue in issue_times:
            if not station_data.empty:
                for hours in (24, 168):
                    window = station_data[station_data.observed_at.ge(issue - pd.Timedelta(hours=hours)) & station_data.observed_at.lt(issue)]
                    # Cada série exige ao menos 75% de horários válidos; faltas continuam explícitas.
                    for variable, operation, unit in (("precipitation_mm", "sum", "mm"), ("temperature_c", "max", "C"),
                                                       ("relative_humidity_pct", "min", "%"), ("wind_ms", "max", "m/s")):
                        observations = window[variable].dropna()
                        sufficient = len(observations) >= int(np.ceil(hours * .75))
                        value = getattr(observations, operation)() if sufficient else np.nan
                        available = window.available_at.max() if window.available_at.notna().all() and len(window) else pd.NaT
                        add(code, f"observed_{variable}_{operation}_{hours}h", value, issue, available, "inmet", unit,
                            str(window.station_code.iloc[0]) if len(window) else "no_observations")
                        add(code, f"observed_{variable}_coverage_{hours}h", len(observations) / hours, issue, available, "inmet", "fraction", "hourly_coverage")
            if not land.empty:
                # Mantém todas as coleções; attach_features aplica disponibilidade antes da emissão.
                available_land = land[land.municipality_code.eq(code) & land.available_at.le(issue) & land.year.le(issue.year)]
                if not available_land.empty:
                    chosen_year = available_land.year.max()
                    chosen = available_land[available_land.year.eq(chosen_year)]
                    chosen = chosen[chosen.available_at.eq(chosen.available_at.max())]
                    if chosen.class_code.duplicated().any():
                        raise ValueError("MapBiomas com revisões ambíguas")
                    for row in chosen.itertuples():
                        add(code, "landcover_class_" + str(row.class_code), row.fraction, issue,
                            row.available_at, "mapbiomas", "fraction", row.collection)
            if not gfs.empty:
                available_gfs = gfs[gfs.available_at.le(issue) & gfs.issued_at.le(issue)
                                    & gfs.valid_at.ge(issue) & gfs.valid_at.lt(issue + pd.Timedelta(hours=24))]
                if not available_gfs.empty:
                    available_gfs = available_gfs[available_gfs.issued_at.eq(available_gfs.issued_at.max())]
                    center = gpd.GeoSeries([centers.loc[idx]], crs=5880).to_crs(4326).iloc[0]
                    grid = available_gfs[["latitude", "longitude"]].drop_duplicates()
                    distances = (grid.latitude - center.y) ** 2 + ((grid.longitude - center.x) * np.cos(np.deg2rad(center.y))) ** 2
                    closest = grid.loc[distances.idxmin()]
                    cell = available_gfs[available_gfs.latitude.eq(closest.latitude) & available_gfs.longitude.eq(closest.longitude)]
                    if cell.duplicated(["variable", "valid_at"]).any():
                        raise ValueError("GFS com revisões ambíguas da mesma grade/ciclo")
                    for variable, name, operation, unit in (("t2m", "forecast_temperature_max_24h", "max", "C"),
                                                            ("r2", "forecast_humidity_min_24h", "min", "%")):
                        values = cell[cell.variable.eq(variable)].copy()
                        if len(values) >= 6:
                            if variable == "t2m":
                                if not values.units.isin(["K"]).all(): raise ValueError("Unidade de temperatura GFS inesperada")
                                values["value"] -= 273.15
                            elif not values.units.isin(["%", "percent"]).all():
                                raise ValueError("Unidade de umidade GFS inesperada")
                            add(code, name, getattr(values.value, operation)(), issue, values.available_at.max(), "gfs", unit, str(values.issued_at.max()))
                    wind = cell[cell.variable.isin(["u10", "v10"])].pivot(index="valid_at", columns="variable", values="value")
                    if {"u10", "v10"} <= set(wind) and len(wind.dropna()) >= 6:
                        add(code, "forecast_wind_max_24h", np.sqrt(wind.u10 ** 2 + wind.v10 ** 2).max(), issue,
                            cell.available_at.max(), "gfs", "m/s", str(cell.issued_at.max()))
                    # tp permanece em Prata: acumulações GRIB exigem teste dos intervalos antes da soma.
    if not rows:
        return {"rows": 0, "status": "no_usable_features_for_period",
                "note": "Fontes ausentes ou sem horários/validade suficientes antes da emissão"}
    frame = pd.DataFrame(rows)
    for c in ("valid_from", "valid_to", "available_at"):
        frame[c] = pd.to_datetime(frame[c], utc=True, errors="coerce")
    identity = {"start": start, "end": end, "codes": codes,
                "inputs": [file_hash(p) for p in inmet_files + land_files + gfs_files],
                "implementation": file_hash(Path(__file__)), "config": cfg.__dict__}
    identifier = digest(json.dumps(identity, sort_keys=True).encode())[:24]
    directory = cfg.path("data/silver/derived_features")
    directory.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(directory / (identifier + ".parquet"), index=False)
    write_json(directory / (identifier + ".json"), {**identity, "created_at": now(), "station_selection": selection,
        "rain_forecast_status": "not_aggregated_pending_interval_validation", "rows": len(frame)})
    write_json(cfg.path("metadata/derived_features_current.json"), {"path": str(directory / (identifier + ".parquet"))})
    index_path = cfg.path("metadata/derived_features_active.json")
    index = json.loads(index_path.read_text(encoding="utf-8")) if index_path.exists() else {}
    batch_key = digest(json.dumps([start, end, sorted(codes)]).encode())
    index[batch_key] = (directory / (identifier + ".parquet")).relative_to(cfg.root).as_posix()
    write_json(index_path, index)
    return {"path": str(directory / (identifier + ".parquet")), "rows": len(frame), "station_selection": selection}
