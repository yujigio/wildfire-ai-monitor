"""Coleta oficial retomável. Bronze imutável, recibos SHA-256 e CRC ZIP."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "entrega_csv/codigo/src"))
from incendios.core import Config, file_hash, now, write_json, lake_lock


def fetch(url: str, destination: Path, source: str, expected_md5=None):
    destination.parent.mkdir(parents=True, exist_ok=True)
    receipt = destination.with_suffix(destination.suffix + ".metadata.json")
    if destination.exists() and receipt.exists():
        saved = json.loads(receipt.read_text(encoding="utf-8"))
        if saved["sha256"] != file_hash(destination):
            raise ValueError(f"Bronze alterada: {destination.name}")
        print(json.dumps({"file": destination.name, "status": "verified_existing"}), flush=True)
        return saved
    temporary = destination.with_suffix(destination.suffix + ".part")
    for attempt in range(3):
        try:
            with requests.get(url, stream=True, timeout=(20, 90)) as response:
                response.raise_for_status()
                total = 0
                sha = hashlib.sha256()
                md5 = hashlib.md5()
                with temporary.open("wb") as output:
                    for block in response.iter_content(1024 * 1024):
                        if not block:
                            continue
                        total += len(block)
                        if total > 300 * 1024 * 1024:
                            raise ValueError("Arquivo excedeu limite de 300 MB")
                        output.write(block)
                        sha.update(block)
                        md5.update(block)
                if response.headers.get("Content-Length") and total != int(response.headers["Content-Length"]):
                    raise ValueError("Download incompleto")
            if expected_md5 and md5.hexdigest() != expected_md5:
                raise ValueError("Checksum oficial divergente")
            with zipfile.ZipFile(temporary) as archive:
                if archive.testzip() is not None:
                    raise ValueError("CRC ZIP inválido")
                members = [{"name": i.filename, "bytes": i.file_size, "crc32": i.CRC}
                           for i in archive.infolist()]
            checksum = sha.hexdigest()
            if destination.exists():
                if file_hash(destination) != checksum:
                    destination = destination.with_name(destination.stem + "-" + checksum[:12] + destination.suffix)
                    receipt = destination.with_suffix(destination.suffix + ".metadata.json")
                else:
                    temporary.unlink()
            if temporary.exists():
                os.replace(temporary, destination)
            saved = {"source": source, "source_url": url, "sha256": checksum, "md5": md5.hexdigest(),
                     "collected_at": now(), "bytes": total, "members": members,
                     "path": str(destination), "source_product": "reference_history" if source == "inpe" else source,
                     "archive_crc_verified": True, "status": "downloaded"}
            write_json(receipt, saved)
            print(json.dumps({"file": destination.name, "bytes": total, "status": "downloaded"}), flush=True)
            return saved
        except (requests.RequestException, ValueError, zipfile.BadZipFile) as exc:
            temporary.unlink(missing_ok=True)
            print(json.dumps({"file": destination.name, "attempt": attempt + 1,
                              "error": type(exc).__name__}), flush=True)
            if attempt == 2:
                raise
            time.sleep(2 ** attempt)


def collect_inpe(cfg, years):
    results = []
    for year in (y for y in years if y < 2025):
        name = f"focos_br_sp_ref_{year}.zip"
        url = "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/anual/EstadosBr_sat_ref/SP/" + name
        results.append(fetch(url, cfg.path("data/bronze/inpe/official_sp/" + name), "inpe"))
    if 2025 in years:
        url = "https://dataserver-coids.inpe.br/queimadas/queimadas/focos/csv/anual/Brasil_sat_ref/focos_br_ref_2025.zip"
        # Mantém o nacional original; a auditoria espacial recorta SP sem editar a Bronze.
        results.append(fetch(url, cfg.path("data/bronze/inpe/official_sp/focos_br_ref_2025.zip"), "inpe"))
    write_json(cfg.path("metadata/official_inpe_downloads.json"), results)
    return results


def collect_inmet(cfg, years):
    results = []
    for year in years:
        url = f"https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"
        results.append(fetch(url, cfg.path(f"data/bronze/inmet/annual/{year}.zip"), "inmet"))
    write_json(cfg.path("metadata/official_inmet_downloads.json"), results)
    return results


def collect_mapbiomas(cfg):
    metadata_url = "https://data.mapbiomas.org/api/datasets/:persistentId/?persistentId=doi:10.58053/MapBiomas/SJZOLT"
    response = requests.get(metadata_url, timeout=60)
    response.raise_for_status()
    metadata = response.json()
    version = metadata["data"]["latestVersion"]
    matches = [i["dataFile"] for i in version["files"] if "MUNICIPALITIES" in i["dataFile"]["filename"]]
    if len(matches) != 1:
        raise ValueError("Dataset municipal ambíguo; revisar versão oficial")
    info = matches[0]
    url = f"https://data.mapbiomas.org/api/access/datafile/{info['id']}"
    checksum = info.get("checksum", {})
    if checksum.get("type") != "MD5":
        raise ValueError("Tipo de checksum oficial não suportado")
    result = fetch(url, cfg.path("data/bronze/mapbiomas/" + info["filename"]), "mapbiomas", checksum["value"])
    write_json(cfg.path("metadata/mapbiomas_dataverse.json"), metadata)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("source", choices=["all", "inpe", "inmet", "ibge", "mapbiomas"])
    parser.add_argument("--start-year", type=int, default=2015)
    parser.add_argument("--end-year", type=int, default=2025)
    parser.add_argument("--lake-root", type=Path, default=ROOT / "workspace")
    args = parser.parse_args()
    if not 2015 <= args.start_year <= args.end_year <= 2025:
        parser.error("O coletor histórico desta entrega atende ao período 2015–2025.")
    cfg = Config(lake_root=str(args.lake_root.resolve()), input_relative="data/bronze/inpe/official_sp",
                 annual_timezone_verified=True)
    from incendios.sources import collect_ibge
    jobs = {"ibge": lambda: collect_ibge(cfg),
            "inpe": lambda: collect_inpe(cfg, range(args.start_year, args.end_year + 1)),
            "inmet": lambda: collect_inmet(cfg, range(args.start_year, args.end_year + 1)),
            "mapbiomas": lambda: collect_mapbiomas(cfg)}
    selected = list(jobs) if args.source == "all" else [args.source]
    with lake_lock(cfg), ThreadPoolExecutor(max_workers=2) as executor:
        futures = {name: executor.submit(jobs[name]) for name in selected}
        statuses = {}
        for name, future in futures.items():
            try:
                result = future.result()
                statuses[name] = {"status": "success", "files": len(result) if isinstance(result, list) else 1}
            except Exception as exc:
                statuses[name] = {"status": "failed", "error": str(exc)[:250]}
    write_json(cfg.path("metadata/official_collection_status.json"), statuses)
    print(json.dumps(statuses, ensure_ascii=False), flush=True)
    if any(v["status"] == "failed" for v in statuses.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
