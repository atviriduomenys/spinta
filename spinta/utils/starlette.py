import math

from starlette.convertors import Convertor, register_url_convertor


class SpintaFloatConvertor(Convertor[float]):
    regex = r"-?[0-9]+(\.[0-9]+)?"

    def convert(self, value: str) -> float:
        return float(value)

    def to_string(self, value: float) -> str:
        value = float(value)
        assert not math.isnan(value), "NaN values are not supported"
        assert not math.isinf(value), "Infinite values are not supported"
        return ("%0.20f" % value).rstrip("0").rstrip(".")


register_url_convertor('spinta_float', SpintaFloatConvertor())
