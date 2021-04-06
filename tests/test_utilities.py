from datetime import datetime
from enum import Enum

import pytest

from coupledmodeldriver.utilities import convert_to_json, convert_value


class ValueTest:
    def __init__(self, value: int):
        self.value = value

    def __eq__(self, other: 'ValueTest') -> bool:
        return self.value == other.value


class FloatTest:
    def __init__(self, value: int):
        self.value = value

    def __str__(self) -> str:
        return f'{self.value}'

    def __int__(self) -> int:
        return int(self.value)

    def __float__(self) -> float:
        return float(self.value)


class IntegerTest:
    def __init__(self, value: int):
        self.value = value

    def __int__(self) -> int:
        return int(self.value)


class EnumerationTest(Enum):
    test_1 = 'test_1'


def test_convert_value():
    result_1 = convert_value('a', str)

    with pytest.raises(ValueError):
        convert_value('a', float)

    result_2 = convert_value(0.55, str)
    result_3 = convert_value(0.55, 'str')
    result_4 = convert_value('0.55', float)
    result_5 = convert_value('0.55', 'float')

    result_6 = convert_value(0.55, int)
    result_7 = convert_value('5', int)

    with pytest.raises(ValueError):
        convert_value('0.55', int)

    result_8 = convert_value('a', [str])
    result_9 = convert_value([1], str)
    result_10 = convert_value([1, 2, '3', '4'], [int])
    result_11 = convert_value([1, 2, '3', '4'], (int, str, float, str))

    with pytest.raises(ValueError):
        convert_value([1, 2, '3', '4'], (int, str))

    with pytest.raises(ValueError):
        convert_value([1, 2, '3', '4'], (int, str, float, str, float))

    result_12 = convert_value(datetime(2021, 3, 26), str)
    result_13 = convert_value('20210326', datetime)

    result_14 = convert_value(5, ValueTest)
    result_15 = convert_value('test_1', EnumerationTest)

    result_16 = convert_value(None, str)

    with pytest.raises((KeyError, ValueError)):
        convert_value(5, EnumerationTest)

    assert result_1 == 'a'
    assert result_2 == '0.55'
    assert result_3 == '0.55'
    assert result_4 == 0.55
    assert result_5 == 0.55

    assert result_6 == 0
    assert result_7 == 5

    assert result_8 == ['a']
    assert result_9 == '[1]'
    assert result_10 == [1, 2, 3, 4]
    assert result_11 == (1, '2', 3.0, '4')

    assert result_12 == '2021-03-26 00:00:00'
    assert result_13 == datetime(2021, 3, 26)

    assert result_14 == ValueTest(5)
    assert result_15 == EnumerationTest.test_1

    assert result_16 is None


def test_convert_values_to_json():
    result_1 = convert_to_json(5)
    result_2 = convert_to_json('5')

    result_3 = convert_to_json(FloatTest(5))
    result_4 = convert_to_json(IntegerTest(5.0))
    result_5 = convert_to_json(FloatTest(5.5))

    result_6 = convert_to_json('test')
    result_7 = convert_to_json(datetime(2021, 3, 26))

    assert result_1 == 5
    assert result_2 == '5'

    assert result_3 == 5.0
    assert result_4 == 5
    assert result_5 == 5.5

    assert result_6 == 'test'
    assert result_7 == '2021-03-26 00:00:00'
