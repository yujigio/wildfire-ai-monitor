"""Variáveis ambientais anuais em lotes, com trilha retrospectiva explícita."""
from __future__ import annotations

import json
from pathlib import Path
import geopandas as gpd
import numpy as np
import pandas as pd

from .core import Config, current_dir, file_hash, now, write_json
from .sources import import_inmet, import_landcover

WEATHER = {"precipitation_mm": ("sum", "mm"), "temperature_c": ("max", "C"),
           "relative_humidity_pct": ("min", "%"), "wind_ms": ("max", "m/s")}
LAND_GROUPS = ["forest", "natural_nonforest", "pasture", "cropland", "forest_plantation",
               "farming_other", "nonvegetated", "water", "not_observed"]


def station_windows(hourly: pd.DataFrame, issues: pd.DatetimeIndex) -> pd.DataFrame:
    """Exclui a hora de emissão; faltas nunca viram chuva zero."""
    hourly = hourly.copy()
    hourly.observed_at = pd.to_datetime(hourly.observed_at, utc=True)
    if hourly.observed_at.duplicated().any():
        raise ValueError("Estação com horário duplicado")
    left = min(issues.min() - pd.Timedelta(days=7), hourly.observed_at.min())
    right = max(issues.max(), hourly.observed_at.max())
    index = pd.date_range(left.floor("h"), right.ceil("h"), freq="h")
    series = hourly.set_index("observed_at").reindex(index)
    result = pd.DataFrame(index=issues)
    for hours in (24, 168):
        for variable, (operation, _) in WEATHER.items():
            window = series[variable].rolling(f"{hours}h", closed="left", min_periods=1)
            count = window.count().reindex(issues).fillna(0)
            value = getattr(window, operation)().reindex(issues)
            name = f"observed_{variable}_{operation}_{hours}h"
            result[name] = value.where(count.ge(np.ceil(.75 * hours)))
            result[name + "_missing"] = result[name].isna()
            result[f"observed_{variable}_coverage_{hours}h"] = count / hours
    return result.reset_index(names="issuance_at")


def prepare_environment_year(cfg: Config, year: int) -> dict:
    """SP inteiro, um ano e uma estação por vez; não inventa disponibilidade passada."""
    directory = current_dir(cfg, "silver")
    if directory is None:
        raise ValueError("Audite INPE/IBGE antes de preparar ambiente")
    bronze = cfg.path(f"data/bronze/inmet/annual/{year}.zip")
    if not bronze.exists():
        raise ValueError(f"INMET {year} não coletado")
    land_csv = cfg.path("data/silver/mapbiomas_official/landcover.csv")
    if not land_csv.exists():
        raise ValueError("Extraia a tabela municipal oficial do MapBiomas")
    inputs = {"weather": file_hash(bronze), "land": file_hash(land_csv),
              "territories": file_hash(directory / "territories.parquet"),
              "implementation": file_hash(Path(__file__))}
    output = cfg.path(f"data/silver/environment_daily/year={year}")
    manifest_path = output / "manifest.json"
    if manifest_path.exists():
        saved = json.loads(manifest_path.read_text(encoding="utf-8"))
        if saved["inputs"] == inputs and saved["sha256"] == file_hash(output / "part.parquet"):
            return {"year": year, "rows": saved["rows"], "status": "verified_existing"}
    imported = cfg.path("data/silver/inmet/" + inputs["weather"][:24] + "/observations.parquet")
    if not imported.exists():
        import_inmet(cfg, bronze)
    met = pd.read_parquet(imported)
    met.observed_at = pd.to_datetime(met.observed_at, utc=True)
    stations = met[["station_code", "latitude", "longitude"]].drop_duplicates()
    if stations.station_code.duplicated().any():
        raise ValueError("Estação deslocada no mesmo ano; revisar localização")
    stations = gpd.GeoDataFrame(stations, geometry=gpd.points_from_xy(stations.longitude, stations.latitude), crs=4326).to_crs(5880)
    municipalities = gpd.read_parquet(directory / "territories.parquet").to_crs(5880)
    issues = (pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D") + pd.Timedelta(hours=cfg.issuance_hour)).tz_localize(cfg.timezone).tz_convert("UTC")
    previous = cfg.path("data/silver/inmet")
    # Apenas dezembro do ano anterior, para janelas que cruzam 1º de janeiro.
    earlier_archive = cfg.path(f"data/bronze/inmet/annual/{year-1}.zip")
    earlier = pd.DataFrame()
    if earlier_archive.exists():
        earlier_path = previous / file_hash(earlier_archive)[:24] / "observations.parquet"
        if earlier_path.exists():
            earlier = pd.read_parquet(earlier_path, filters=[("observed_at", ">=", pd.Timestamp(f"{year-1}-12-24", tz="UTC"))])
    land_import = import_landcover(cfg, land_csv, "10.1", "2026-02-19")
    land = pd.read_parquet(Path(land_import["path"]) / "landcover.parquet")
    land = land[land.year.eq(year - 1)].pivot(index="municipality_code", columns="class_code", values="fraction")
    land = land.reindex(columns=LAND_GROUPS, fill_value=0).fillna(0)
    rows, selections, cache = [], [], {}
    for row in municipalities.itertuples():
        code = row.municipality_code
        distance = stations.geometry.distance(row.geometry.representative_point())
        nearest = distance.idxmin()
        station_code = str(stations.loc[nearest, "station_code"])
        distance_m = float(distance.loc[nearest])
        if distance_m <= 60000:
            if station_code not in cache:
                hourly = met[met.station_code.eq(station_code)]
                if not earlier.empty:
                    hourly = pd.concat([earlier[earlier.station_code.eq(station_code)], hourly], ignore_index=True)
                cache[station_code] = station_windows(hourly, issues)
            frame = cache[station_code].copy()
            selections.append({"municipality_code": code, "station_code": station_code, "distance_m": distance_m})
        else:
            frame = pd.DataFrame({"issuance_at": issues})
            for hours in (24, 168):
                for variable, (operation, _) in WEATHER.items():
                    name = f"observed_{variable}_{operation}_{hours}h"
                    frame[name] = np.nan
                    frame[name + "_missing"] = True
                    frame[f"observed_{variable}_coverage_{hours}h"] = 0.0
        frame.insert(0, "municipality_code", code)
        for group in LAND_GROUPS:
            name = "landcover_" + group
            frame[name] = float(land.loc[code, group]) if code in land.index else np.nan
            frame[name + "_missing"] = frame[name].isna()
        rows.append(frame)
    result = pd.concat(rows, ignore_index=True)
    for col in result.select_dtypes(include="float"):
        result[col] = result[col].astype("float32")
    output.mkdir(parents=True, exist_ok=True)
    temporary = output / "part.parquet.tmp"
    result.to_parquet(temporary, index=False, compression="zstd")
    temporary.replace(output / "part.parquet")
    manifest = {"year": year, "rows": len(result), "municipalities": len(municipalities),
                "inputs": inputs, "sha256": file_hash(output / "part.parquet"), "created_at": now(),
                "track": "retrospective", "historical_weather_availability_known": False,
                "landcover_available_at": "2026-02-19T00:00:00Z", "landcover_reference_year": year - 1,
                "landcover_hindsight": True, "operational_use_allowed": False,
                "station_method": "nearest representative point, projected EPSG:5880, <=60km",
                "station_selection": selections, "station_covered_municipalities": len(selections),
                "weather_units": {key: value[1] for key, value in WEATHER.items()},
                "minimum_hourly_valid_fraction": .75,
                "quality": {column: int(result[column].sum()) for column in result if column.endswith("_missing")}}
    write_json(manifest_path, manifest)
    return {"year": year, "rows": len(result), "station_covered_municipalities": len(selections), "status": "prepared"}
