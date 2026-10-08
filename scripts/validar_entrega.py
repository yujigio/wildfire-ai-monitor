"""Validate the original delivery against every published SHA-256."""
from pathlib import Path
import hashlib, json
root = Path(__file__).resolve().parents[1] / 'entrega_csv'
manifest = json.loads((root/'MANIFESTO_ENTREGA.json').read_text(encoding='utf-8'))
errors = []
for name, expected in manifest['files_sha256'].items():
    path = root/name
    if not path.is_file() or hashlib.sha256(path.read_bytes()).hexdigest() != expected:
        errors.append(name)
if errors:
    raise SystemExit('Arquivos ausentes ou divergentes: ' + ', '.join(errors))
print(f"Integridade confirmada: {len(manifest['files_sha256'])} arquivos.")
