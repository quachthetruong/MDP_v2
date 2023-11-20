from typing import List


class MinerException(Exception):
    def __init__(self, msg: str):
        self.msg = msg


class InvalidNode(Exception):
    def __init__(self, errors: List[str]):
        self.errors = errors


class InvalidStage(Exception):
    def __init__(self, errors: List[str]):
        self.errors = errors


class InvalidCode(Exception):
    def __init__(self, errors: List[str]):
        self.errors = errors

