import py_compile
from pathlib import Path

import logging
import sys

def isCompileFileSuccess(file_path:Path)->bool:
    try:
        py_compile.compile(file_path, doraise=True)
        return True
    except py_compile.PyCompileError:
        return False