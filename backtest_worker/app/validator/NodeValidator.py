from schemas.miner_unit import Node
import logging


class NodeValidator:
    @staticmethod
    def validate(node: Node):
        assert isinstance(
            node, Node), f"return be instance of Node but got {type(node)}"
        assert node.dataframe is not None, "node dataframe is not None"
        if node.dataframe.empty:
            return
        assert 'symbol_' in node.dataframe and 'indexed_timestamp_' in node.dataframe, "symbol_ or indexed_timestamp_ not in dataframe"

    @staticmethod
    def validate_dict(result):
        assert isinstance(result, dict), "get_inputs must return dict of Node"
        for key, node in result.items():
            NodeValidator.validate(node)

    @staticmethod
    def validate_symbol(node: Node, symbol: str):
        # logging.info(f"node symbol :{node} {symbol}")
        NodeValidator.validate(node)
        exotic = -1
        for i in range(len(node.dataframe)):
            if node.dataframe['symbol_'].iloc[i] != symbol:
                exotic = i
                break
        assert exotic == - \
            1, f"symbol_ in dataframe must be {symbol} but got {node.dataframe['symbol_'].iloc[exotic]}"
