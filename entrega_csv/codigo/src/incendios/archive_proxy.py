"""Rótulos de registro publicado, distintos de cobertura observacional confirmada.

Os negativos significam ausência no arquivo verificado, nunca ausência de fogo.
Este modo existe somente para investigação retrospectiva e não libera publicação.
"""
from __future__ import annotations
import json
from pathlib import Path
import numpy as np
import pandas as pd
from .core import Config, current_dir, file_hash, now, write_json

DISCLAIMER = "ausência no arquivo oficial publicado não confirma ausência de fogo nem imageamento completo"


def register_archives(cfg: Config) -> dict:
    records = []
    for year in range(2015, 2026):
        name = f"focos_br_sp_ref_{year}.zip" if year < 2025 else "focos_br_ref_2025.zip"
        path = cfg.path("data/bronze/inpe/official_sp/" + name)
        receipt = path.with_suffix(".zip.metadata.json")
        if not path.exists() or not receipt.exists():
            raise ValueError(f"Arquivo oficial e recibo exigidos: {year}")
        info = json.loads(receipt.read_text(encoding="utf-8"))
        url = info["source_url"]
        if not url.startswith("https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/anual/"):
            raise ValueError("Origem não corresponde ao arquivo oficial anual")
        if not info.get("archive_crc_verified") or info["sha256"] != file_hash(path):
            raise ValueError("Integridade do arquivo não comprovada")
        records.append({"year": year, "relative_path": path.relative_to(cfg.root).as_posix(),
                        "sha256": info["sha256"], "source_url": url})
    silver = current_dir(cfg, "silver")
    if silver is None:
        raise ValueError("Auditoria oficial exigida")
    inventory = json.loads((silver / "inventory.json").read_text(encoding="utf-8"))
    valid_hashes = {r["sha256"] for r in records}
    if not inventory or any(i.get("file_sha256") not in valid_hashes or i["status"] != "inspected" for i in inventory):
        raise ValueError("Prata deve conter somente os arquivos oficiais verificados")
    if not cfg.annual_timezone_verified or cfg.annual_source_timezone != "UTC":
        raise ValueError("Horário do arquivo anual exige revisão da documentação oficial")
    result = {"created_at": now(), "silver_snapshot": silver.name, "product": "reference_history",
              "target_mode": "published_archive", "disclaimer": DISCLAIMER,
              "observation_coverage_approved": False, "operational_use_allowed": False,
              "archives": records,
              "timezone_evidence": "https://data.inpe.br/queimadas/faq/ (DataHora GMT); https://dataserver-coids.inpe.br/queimadas/queimadas/focos/documentos/Curadoria-Governanca-Dados-FocosFogoAtivo.pdf (data_pas UTC)",
              "excluded_utc_days": ["2024-08-15", "2024-08-17"],
              "exclusion_evidence": "https://data.inpe.br/queimadas/avisos/; exclusão conservadora em todo SP, sem afirmar que toda a área foi afetada",
              "unreported_outages_possible": True}
    existing = cfg.path("metadata/archive_proxy.json")
    if existing.exists():
        previous = json.loads(existing.read_text(encoding="utf-8"))
        without_time = lambda value: {k: v for k, v in value.items() if k != "created_at"}
        if without_time(previous) == without_time(result):
            return previous
    write_json(existing, result)
    return result


def archive_mask(cfg: Config, product: str, starts, ends):
    info = json.loads(cfg.path("metadata/archive_proxy.json").read_text(encoding="utf-8"))
    if info["silver_snapshot"] != current_dir(cfg, "silver").name or product != info["product"]:
        raise ValueError("Manifesto de arquivos incompatível com a Prata atual")
    mask = np.zeros(len(starts), dtype=bool)
    for record in info["archives"]:
        if file_hash(cfg.path(record["relative_path"])) != record["sha256"]:
            raise ValueError("Arquivo oficial mudou após registro")
    years = sorted(r["year"] for r in info["archives"])
    # Une anos adjacentes sem liberar janelas fora dos arquivos.
    groups = []
    for year in years:
        if groups and year == groups[-1][-1] + 1:
            groups[-1].append(year)
        else:
            groups.append([year])
    for group in groups:
        left = pd.Timestamp(f"{group[0]}-01-01", tz="UTC")
        right = pd.Timestamp(f"{group[-1]+1}-01-01", tz="UTC")
        mask |= (starts >= left) & (ends <= right)
    for day in info["excluded_utc_days"]:
        left = pd.Timestamp(day, tz="UTC")
        mask &= ~((starts < left + pd.Timedelta(days=1)) & (ends > left))
    return mask, info
