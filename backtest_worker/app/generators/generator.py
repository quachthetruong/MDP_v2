import importlib
import logging
import traceback

# from fastapi import HTTPException, status
from schemas.miner import MinerCatalog
from common.template_loader import TemplateLoader
from commons.utils import camel_case


def get_class(miner_name: str):
    # try:
    module = importlib.import_module(f'miners.generate.{miner_name}')
    # except SyntaxError as e:
    #     raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED,
    #                         detail=[e.msg]+traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)[-3:])
    miner_class = camel_case(miner_name)
    return getattr(module, miner_class)

# def clean_file_generated(miner_name:str):
#     os.remove(f"miners/generate/{miner_name}.py")

# def rename_file(file_name:str,new_name:str):
#     os.rename(f"miners/generate/{file_name}.py",f"miners/generate/{new_name}.py")


def generate_miner(minerCatalog: MinerCatalog, get_inputs_str: str = None, process_per_symbol_str: str = None, miner_generate_path="miners/generate/"):
    loader = TemplateLoader()
    args = {'metadata': minerCatalog.metadata, 'spec': minerCatalog.spec, 'get_inputs_str': get_inputs_str, 'process_per_symbol_str': process_per_symbol_str,
            "func": {"camel_case": camel_case}}
    minerCode = loader.render("catalog/miner_back_test_code.tpl", **args)
    file_path = f"{miner_generate_path}{minerCatalog.metadata.name}.py"
    with open(file_path, "w+") as f:
        f.write(minerCode)
    return minerCatalog.metadata.name
