from schemas.miner_unit import Stage


class StageValidator:
    @staticmethod
    def validate(stage: Stage):
        assert isinstance(stage, Stage), "return be instance of Stage"
