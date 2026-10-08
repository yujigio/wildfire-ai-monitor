from __future__ import annotations

import hashlib
import json
import os
import platform
import shutil
import sys
import uuid
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

VERSION = "0.1.0"


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def digest(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    os.replace(tmp, path)


@dataclass
class Config:
    lake_root: str = field(default_factory=lambda: os.environ.get("INCENDIOS_LAKE_ROOT", str(Path(__file__).resolve().parents[2])))
    input_relative: str = "data/bronze/inpe/sp"
    boundaries_relative: str = "data/bronze/ibge/SP_Municipios_2024.zip"
    boundary_code_column: str = "CD_MUN"
    boundary_name_column: str = "NM_MUN"
    timezone: str = "America/Sao_Paulo"
    annual_source_timezone: str = "UTC"
    annual_timezone_verified: bool = False
    issuance_hour: int = 9
    state_code: str = "35"
    pilot_codes: list[str] = field(default_factory=lambda: ["3534708"])
    firms_product: str = "VIIRS_SNPP_NRT"
    bbox: list[float] = field(default_factory=lambda: [-53.2, -25.4, -44, -19.7])
    max_download_mb: int = 200
    source_max_age_hours: int = 24

    @classmethod
    def load(cls, path: str | None = None):
        cfg = cls(**json.loads(Path(path).read_text(encoding="utf-8"))) if path else cls()
        if not 0 <= cfg.issuance_hour <= 23:
            raise ValueError("issuance_hour deve estar entre 0 e 23")
        return cfg

    @property
    def root(self) -> Path:
        return Path(self.lake_root).resolve()

    def path(self, relative: str) -> Path:
        p = (self.root / relative).resolve()
        if not p.is_relative_to(self.root):
            raise ValueError("Caminho fora do Data Lake")
        return p


@contextmanager
def lake_lock(cfg: Config):
    """Serializa mutações para evitar duas publicações concorrentes."""
    path = cfg.path("metadata/pipeline.lock")
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise RuntimeError("Há outra execução ou lock após interrupção. Consulte metadata/pipeline.lock.")
    try:
        os.write(fd, json.dumps({"pid": os.getpid(), "started": now()}).encode())
        os.close(fd)
        yield
    finally:
        path.unlink(missing_ok=True)


@contextmanager
def run_log(cfg: Config, task: str, params: dict):
    run = {"id": uuid.uuid4().hex, "task": task, "started": now(), "params": params,
           "version": VERSION, "python": sys.version, "platform": platform.platform(),
           "config": asdict(cfg), "status": "running"}
    path = cfg.path(f"logs/runs/{run['id']}.json")
    write_json(path, run)
    try:
        yield run
        run["status"] = "success"
    except Exception as exc:
        run["status"] = "failed"
        run["error"] = str(exc)
        raise
    finally:
        run["finished"] = now()
        write_json(path, run)


def current_dir(cfg: Config, layer: str) -> Path | None:
    pointer = cfg.path(f"metadata/{layer}_current.json")
    if not pointer.exists():
        return None
    value = json.loads(pointer.read_text(encoding="utf-8"))
    directory = cfg.path(value["relative_path"])
    return directory if directory.is_dir() else None


def publish_dir(cfg: Config, layer: str, staging: Path, snapshot_id: str) -> Path:
    """Publica o pointer somente após todos os arquivos estarem completos."""
    dest = cfg.path(f"data/{layer}/snapshots/{snapshot_id}")
    if not dest.exists():
        dest.parent.mkdir(parents=True, exist_ok=True)
        staging.rename(dest)
    else:
        shutil.rmtree(staging)
    write_json(cfg.path(f"metadata/{layer}_current.json"),
               {"relative_path": dest.relative_to(cfg.root).as_posix(), "snapshot_id": snapshot_id,
                "published_at": now()})
    return dest


def read_table(cfg: Config, name: str, layer: str = "silver") -> pd.DataFrame:
    directory = current_dir(cfg, layer)
    if directory is None or not (directory / f"{name}.parquet").exists():
        return pd.DataFrame()
    return pd.read_parquet(directory / f"{name}.parquet")
