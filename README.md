# Wildfire AI Monitor — coleta e tratamento dos CSVs

Parte do projeto compartilhada com a equipe: coleta dos arquivos oficiais e tratamento das tabelas de focos, meteorologia, território e uso do solo em São Paulo. A entrega original está preservada em `entrega_csv/`.

## Conteúdo

- `scripts/coletar_csv.py`: coletor retomável dos ZIPs/CSVs oficiais INPE, INMET, IBGE e MapBiomas, com verificação de integridade e recibos.
- `entrega_csv/codigo/`: código de auditoria, importação, normalização e construção das bases Silver e Gold.
- `entrega_csv/data/`: cópia das bases tratadas compartilhadas; a Gold contém 2.591.610 linhas de município/dia, 645 municípios, 2015–2025.
- `entrega_csv/DICIONARIO_BASES.json`: arquivos, colunas e tipos.
- `entrega_csv/MANIFESTO_ENTREGA.json`: hashes do pacote original.
- `entrega_csv/LEIA_ME.md`: interpretação das bases e instruções de tratamento.

## Preparar o ambiente

Use Python 3.12 ou superior. Na raiz deste repositório:

```sh
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/macOS: source .venv/bin/activate
python -m pip install -r requirements.txt
python scripts/validar_entrega.py
```

## Coletar os arquivos oficiais

Os downloads são gravados em `workspace/`, separada das bases entregues. Para coletar apenas os focos CSV do INPE, ou todas as fontes da entrega:

```sh
python scripts/coletar_csv.py inpe --start-year 2015 --end-year 2025
python scripts/coletar_csv.py all --start-year 2015 --end-year 2025
```

INPE e INMET atendem ao intervalo selecionado. IBGE usa a malha municipal de SP de 2024; MapBiomas usa o dataset municipal publicado na fonte oficial. Para outro destino, informe `--lake-root /caminho/da/pasta`.

## Tratar os dados

Depois de reunir as entradas e os recibos necessários descritos em `entrega_csv/LEIA_ME.md`:

```sh
python entrega_csv/codigo/scripts/tratar.py --help
python entrega_csv/codigo/scripts/tratar.py audit --lake-root workspace
```

Os demais comandos disponíveis são `import-inmet`, `prepare-environment`, `register-archives` e `gold`. Os exemplos completos estão no guia da entrega. A Bronze não é incluída neste repositório; os arquivos tratados já podem ser consultados sem executar novamente a coleta.

## Fontes e interpretação

As fontes oficiais são [INPE](https://data.inpe.br/queimadas/), [INMET](https://portal.inmet.gov.br/dadoshistoricos), [IBGE](https://www.ibge.gov.br/geociencias/organizacao-do-territorio/estrutura-territorial/15774-malhas.html) e [MapBiomas](https://data.mapbiomas.org/). Preserve a atribuição, a edição e os manifestos de cada produto.

O alvo desta base retrospectiva é registro de detecção de foco no produto histórico publicado. Cobertura desconhecida e ausências estão identificadas. As tabelas Silver, ambientais e Gold são etapas do processamento e não devem ser concatenadas como bases independentes.
