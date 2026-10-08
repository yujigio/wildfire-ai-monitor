# Código de tratamento e bases tratadas — Incêndios SP

Entrega para consulta e compartilhamento. Inclui código de tratamento e cópias das bases tratadas existentes. Nenhum arquivo original foi alterado e nenhum modelo foi treinado durante a montagem.

## Bases incluídas

- **Base final município/dia:** `data/gold/snapshots/2f3f2c6d958ca84c77d0cf24/municipality_day`, com 2.591.610 linhas, 645 municípios e 2015–2025. Contém variáveis meteorológicas, uso do solo, alvos e divisões temporais.
- **Observações e território:** `data/silver/snapshots/3d3b799de0e978c474a2f3a6`, com focos normalizados, municípios, limites, inventário, cobertura e exceções. O arquivo de exceções inclui registros rejeitados ou sinalizados; não deve ser somado aos focos válidos.
- **Variáveis ambientais:** `data/silver/environment_daily`, uma tabela por ano, com agregados meteorológicos e uso do solo por município/emissão. Não inclui toda a série horária de estações.
- **Uso do solo:** `data/silver/mapbiomas_official/landcover.csv` e seu manifesto.
- **Rastreabilidade:** manifestos, ponteiros de versão, hashes e relatório de validação no Colab em `metadata`.

Os arquivos `.parquet` preservam tipos e ocupam menos espaço que CSV. O território em Parquet é geoespacial; também está disponível em GeoJSON. `DICIONARIO_BASES.json` lista os arquivos, quantidades de linhas, colunas e tipos. As diferentes tabelas representam etapas do mesmo processamento, não bases independentes para concatenar.

## Código de tratamento

`codigo/src/incendios` contém auditoria, importação, normalização, variáveis ambientais e construção da base final. `codigo/scripts/tratar.py` oferece apenas comandos de tratamento, sem treinamento.

Use Python 3.12 ou superior em ambiente separado. As versões diretas estão em `codigo/requirements.txt`; bibliotecas de geoprocessamento podem exigir dependências do sistema. Exemplo de instalação, executado a partir desta pasta:

```sh
python -m pip install -r codigo/requirements.txt
python codigo/scripts/tratar.py --help
```

Para **consultar** a base pronta, não é necessário reprocessá-la:

```python
import duckdb
from pathlib import Path
root = Path('.')  # pasta extraída desta entrega
pointer = __import__('json').loads((root/'metadata/gold_current.json').read_text())
pattern = str(root/pointer['relative_path']/'municipality_day/**/*.parquet')
con = duckdb.connect()
con.execute("SET memory_limit='512MB'")
table = con.read_parquet(pattern)
print(table.limit(5).df())
```

## Reprocessamento e entradas necessárias

Esta entrega contém as bases tratadas, não a Bronze completa. Para reproduzir o tratamento desde os originais, obter separadamente os arquivos oficiais e posicioná-los no `--lake-root` escolhido: INPE em `data/bronze/inpe/official_sp`, malha IBGE em `data/bronze/ibge/SP_Municipios_2024.zip` e ZIPs INMET em `data/bronze/inmet/annual/<ano>.zip`. O registro dos arquivos publicados também depende dos recibos de coleta e das evidências de cobertura/procedência do projeto. Para meteorologia de janeiro, preservar os dados necessários do ano anterior. O CSV municipal de uso do solo já acompanha esta entrega.

Exemplos, somente após reunir essas entradas; prefira uma pasta de trabalho separada das bases entregues:

```sh
python codigo/scripts/tratar.py audit --lake-root /caminho/da/pasta_de_trabalho
python codigo/scripts/tratar.py import-inmet --lake-root /caminho/da/pasta_de_trabalho --archive /caminho/2015.zip
python codigo/scripts/tratar.py prepare-environment --lake-root /caminho/da/pasta_de_trabalho --year 2015
python codigo/scripts/tratar.py register-archives --lake-root /caminho/da/pasta_de_trabalho
python codigo/scripts/tratar.py gold --lake-root /caminho/da/pasta_de_trabalho --start 2015-01-01 --end 2025-12-31
```

## Interpretação

O dataset é retrospectivo. O alvo é registro de detecção no produto histórico publicado; não equivale a incêndio confirmado. Ausências e cobertura desconhecida permanecem identificadas. A integridade e a estrutura foram validadas no Colab, sem certificar desempenho preditivo ou previsão operacional. O teste de 2024–2025 já foi consultado em experimento anterior.

Sem credenciais, tokens, modelos treinados, notebooks pessoais ou dados brutos volumosos. Para qualquer futuro treinamento deste projeto, usar somente o Colab.
