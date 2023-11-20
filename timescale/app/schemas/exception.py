class MinerException(Exception):
    def __init__(self, msg: str):
        self.msg = msg


class InvalidNode(Exception):
    def __init__(self, msg: str):
        self.msg = msg


class InvalidStage(Exception):
    def __init__(self, msg: str):
        self.msg = msg


class InvalidCode(Exception):
    def __init__(self, msg: str):
        self.msg = msg
