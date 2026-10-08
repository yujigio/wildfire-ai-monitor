"""Tratamento somente: não inclui comandos de treino."""
import argparse, json, sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'codigo/src'))
from incendios.core import Config, lake_lock
p=argparse.ArgumentParser()
p.add_argument('etapa',choices=['audit','import-inmet','prepare-environment','register-archives','gold'])
p.add_argument('--lake-root',default=str(ROOT))
p.add_argument('--year',type=int)
p.add_argument('--archive',type=Path)
p.add_argument('--start',default='2015-01-01')
p.add_argument('--end',default='2025-12-31')
a=p.parse_args()
cfg=Config(lake_root=a.lake_root,input_relative='data/bronze/inpe/official_sp',annual_timezone_verified=True)
with lake_lock(cfg):
    if a.etapa=='audit':
        from incendios.audit import audit
        result=audit(cfg)
    elif a.etapa=='import-inmet':
        assert a.archive,'Informe --archive'
        from incendios.sources import import_inmet
        result=import_inmet(cfg,a.archive)
    elif a.etapa=='prepare-environment':
        assert a.year,'Informe --year'
        from incendios.environment import prepare_environment_year
        result=prepare_environment_year(cfg,a.year)
    elif a.etapa=='register-archives':
        from incendios.archive_proxy import register_archives
        result=register_archives(cfg)
    else:
        from incendios.gold import build_gold
        result=build_gold(cfg,a.start,a.end,product='reference_history',track='retrospective',target_mode='published_archive')
print(json.dumps(result,default=str,ensure_ascii=False,indent=2))
