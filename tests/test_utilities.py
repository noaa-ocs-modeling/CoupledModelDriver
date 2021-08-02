from datetime import datetime, timedelta
from enum import Enum
import os

from pyproj import CRS
import pytest

from coupledmodeldriver.utilities import convert_to_json, convert_value, create_symlink
from tests import INPUT_DIRECTORY, OUTPUT_DIRECTORY, REFERENCE_DIRECTORY


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
    str_1 = convert_value('a', str)

    with pytest.raises(ValueError):
        convert_value('a', float)

    str_2 = convert_value(0.55, str)
    str_3 = convert_value(0.55, 'str')

    float_1 = convert_value('0.55', float)
    float_2 = convert_value('0.55', 'float')

    int_1 = convert_value(0.55, int)
    int_2 = convert_value('5', int)

    with pytest.raises(ValueError):
        convert_value('0.55', int)

    list_1 = convert_value('a', [str])
    list_2 = convert_value([1], str)
    list_3 = convert_value([1, 2, '3', '4'], [int])
    list_4 = convert_value([1, 2, '3', '4'], (int, str, float, str))

    with pytest.raises(ValueError):
        convert_value([1, 2, '3', '4'], (int, str))

    with pytest.raises(ValueError):
        convert_value([1, 2, '3', '4'], (int, str, float, str, float))

    datetime_1 = convert_value(datetime(2021, 3, 26), str)
    datetime_2 = convert_value('20210326', datetime)

    timedelta_1 = convert_value(timedelta(hours=1), str)
    timedelta_2 = convert_value(timedelta(hours=1), float)
    timedelta_3 = convert_value('00:00:00:00', timedelta)
    timedelta_4 = convert_value('01:13:20:00', timedelta)

    class_1 = convert_value(5, ValueTest)
    class_2 = convert_value('test_1', EnumerationTest)

    none_1 = convert_value(None, str)

    crs_1 = convert_value(CRS.from_epsg(4326), str)
    crs_2 = convert_value(CRS.from_epsg(4326), int)
    crs_3 = convert_value(CRS.from_epsg(4326), dict)

    with pytest.raises((KeyError, ValueError)):
        convert_value(5, EnumerationTest)

    assert str_1 == 'a'
    assert str_2 == '0.55'
    assert str_3 == '0.55'

    assert float_1 == 0.55
    assert float_2 == 0.55

    assert int_1 == 0
    assert int_2 == 5

    assert list_1 == ['a']
    assert list_2 == '[1]'
    assert list_3 == [1, 2, 3, 4]
    assert list_4 == (1, '2', 3.0, '4')

    assert datetime_1 == '2021-03-26 00:00:00'
    assert datetime_2 == datetime(2021, 3, 26)

    assert timedelta_1 == '01:00:00.0'
    assert timedelta_2 == 3600
    assert timedelta_3 == timedelta(seconds=0)
    assert timedelta_4 == timedelta(days=1, hours=13, minutes=20, seconds=0)

    assert class_1 == ValueTest(5)
    assert class_2 == EnumerationTest.test_1

    assert none_1 is None

    if os.name == 'nt':
        reference_crs_wkt = 'GEOGCRS["WGS 84",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1]]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],CS[ellipsoidal,2],AXIS["geodetic latitude (Lat)",north,ORDER[1],ANGLEUNIT["degree",0.0174532925199433]],AXIS["geodetic longitude (Lon)",east,ORDER[2],ANGLEUNIT["degree",0.0174532925199433]],USAGE[SCOPE["Horizontal component of 3D system."],AREA["World."],BBOX[-90,-180,90,180]],ID["EPSG",4326]]'
        reference_crs_json = {
            '$schema': 'https://proj.org/schemas/v0.2/projjson.schema.json',
            'area': 'World.',
            'bbox': {
                'east_longitude': 180,
                'north_latitude': 90,
                'south_latitude': -90,
                'west_longitude': -180,
            },
            'coordinate_system': {
                'axis': [
                    {
                        'abbreviation': 'Lat',
                        'direction': 'north',
                        'name': 'Geodetic latitude',
                        'unit': 'degree',
                    },
                    {
                        'abbreviation': 'Lon',
                        'direction': 'east',
                        'name': 'Geodetic longitude',
                        'unit': 'degree',
                    },
                ],
                'subtype': 'ellipsoidal',
            },
            'datum': {
                'ellipsoid': {
                    'inverse_flattening': 298.257223563,
                    'name': 'WGS 84',
                    'semi_major_axis': 6378137,
                },
                'name': 'World Geodetic System 1984',
                'type': 'GeodeticReferenceFrame',
            },
            'id': {'authority': 'EPSG', 'code': 4326},
            'name': 'WGS 84',
            'scope': 'Horizontal component of 3D system.',
            'type': 'GeographicCRS',
        }
    else:
        reference_crs_wkt = 'GEOGCRS["WGS 84",ENSEMBLE["World Geodetic System 1984 ensemble",MEMBER["World Geodetic System 1984 (Transit)"],MEMBER["World Geodetic System 1984 (G730)"],MEMBER["World Geodetic System 1984 (G873)"],MEMBER["World Geodetic System 1984 (G1150)"],MEMBER["World Geodetic System 1984 (G1674)"],MEMBER["World Geodetic System 1984 (G1762)"],ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1]],ENSEMBLEACCURACY[2.0]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],CS[ellipsoidal,2],AXIS["geodetic latitude (Lat)",north,ORDER[1],ANGLEUNIT["degree",0.0174532925199433]],AXIS["geodetic longitude (Lon)",east,ORDER[2],ANGLEUNIT["degree",0.0174532925199433]],USAGE[SCOPE["Horizontal component of 3D system."],AREA["World."],BBOX[-90,-180,90,180]],ID["EPSG",4326]]'
        reference_crs_json = {
            '$schema': 'https://proj.org/schemas/v0.2/projjson.schema.json',
            'type': 'GeographicCRS',
            'name': 'WGS 84',
            'datum_ensemble': {
                'accuracy': '2.0',
                'name': 'World Geodetic System 1984 ensemble',
                'ellipsoid': {
                    'name': 'WGS 84',
                    'semi_major_axis': 6378137,
                    'inverse_flattening': 298.257223563,
                },
                'id': {'authority': 'EPSG', 'code': 6326,},
                'members': [
                    {
                        'id': {'authority': 'EPSG', 'code': 1166,},
                        'name': 'World Geodetic System 1984 (Transit)',
                    },
                    {
                        'id': {'authority': 'EPSG', 'code': 1152,},
                        'name': 'World Geodetic System 1984 (G730)',
                    },
                    {
                        'id': {'authority': 'EPSG', 'code': 1153,},
                        'name': 'World Geodetic System 1984 (G873)',
                    },
                    {
                        'id': {'authority': 'EPSG', 'code': 1154,},
                        'name': 'World Geodetic System 1984 (G1150)',
                    },
                    {
                        'id': {'authority': 'EPSG', 'code': 1155,},
                        'name': 'World Geodetic System 1984 (G1674)',
                    },
                    {
                        'id': {'authority': 'EPSG', 'code': 1156,},
                        'name': 'World Geodetic System 1984 (G1762)',
                    },
                ],
            },
            'coordinate_system': {
                'subtype': 'ellipsoidal',
                'axis': [
                    {
                        'name': 'Geodetic latitude',
                        'abbreviation': 'Lat',
                        'direction': 'north',
                        'unit': 'degree',
                    },
                    {
                        'name': 'Geodetic longitude',
                        'abbreviation': 'Lon',
                        'direction': 'east',
                        'unit': 'degree',
                    },
                ],
            },
            'scope': 'Horizontal component of 3D system.',
            'area': 'World.',
            'bbox': {
                'south_latitude': -90,
                'west_longitude': -180,
                'north_latitude': 90,
                'east_longitude': 180,
            },
            'id': {'authority': 'EPSG', 'code': 4326,},
        }

    assert crs_1 == reference_crs_wkt
    assert crs_2 == 4326
    assert crs_3 == reference_crs_json


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


def test_create_symlink():
    input_filename = INPUT_DIRECTORY / 'symlink_test.txt'
    output_filename = OUTPUT_DIRECTORY / input_filename.name
    reference_filename = REFERENCE_DIRECTORY / input_filename.name

    create_symlink(input_filename, output_filename)

    with open(output_filename) as output_file:
        with open(reference_filename) as reference_file:
            assert output_file.read() == reference_file.read()
