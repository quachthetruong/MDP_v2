import logging
from os import path

from jinja2 import Environment, FileSystemLoader
import pathlib


class TemplateLoader:
    def __init__(self, template_path="templates"):
        # root_path = Variable.get("ROOT_PATH")
        root_path="/app"
        tmp_path = path.join(root_path, template_path)
        # logging.info(tmp_path)
        self.loader = FileSystemLoader(tmp_path)
        self.env = Environment(loader=self.loader)

    def get_template(self, template_name):
        return self.env.get_template(template_name)

    def render(self, template_name, **kwargs):
        template = self.get_template(template_name)
        if "func" in kwargs:
            template.globals.update(kwargs["func"])
        return template.render(**kwargs)
