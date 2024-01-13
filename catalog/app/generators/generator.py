import importlib
import logging
from schemas.miner import Code, Miner
from commons.template_loader import TemplateLoader
from commons.utils import camel_case
import os


def get_class(miner_name: str):
    module = importlib.import_module(f'miners.generate.{miner_name}')
    miner_class = camel_case(miner_name)
    return getattr(module, miner_class)


def generate_miner(minerCatalog: Miner, code:Code, miner_generate_path="miners/"):
    loader = TemplateLoader()
    args = {'metadata': minerCatalog.metadata, 'spec': minerCatalog.spec, 'get_inputs_str': code.get_input, 'process_per_symbol_str': code.process_per_symbol,
            "func": {"camel_case": camel_case}}
    minerCode = loader.render("miner_code_full.tpl", **args)
    file_path = f"{miner_generate_path}{minerCatalog.metadata.name}.py"
    logging.info(f"generate miner {minerCatalog.metadata.name} to {file_path}")
    with open(file_path, "w+") as f:
        f.write(minerCode)
    return file_path
