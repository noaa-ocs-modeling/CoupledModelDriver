from datetime import datetime, timedelta
from enum import Enum, EnumMeta
import json
import logging
import os
from os import PathLike
from pathlib import Path
import shutil
import sys
from typing import Any, Collection, Iterable, Mapping, Union

from dateutil.parser import parse as parse_date
import numpy
from pyproj import CRS, Geod, Transformer
from shapely.geometry import Point


def repository_root(path: PathLike = None) -> Path:
    if path is None:
        path = __file__
    if not isinstance(path, Path):
        path = Path(path)
    if path.is_file():
        path = path.parent
    if '.git' in (child.name for child in path.iterdir()) or path == path.parent:
        return path
    else:
        return repository_root(path.parent)


def get_logger(
    name: str,
    log_filename: PathLike = None,
    file_level: int = None,
    console_level: int = None,
    log_format: str = None,
) -> logging.Logger:
    if file_level is None:
        file_level = logging.DEBUG
    if console_level is None:
        console_level = logging.INFO
    logger = logging.getLogger(name)

    # check if logger is already configured
    if logger.level == logging.NOTSET and len(logger.handlers) == 0:
        # check if logger has a parent
        if '.' in name:
            logger.parent = get_logger(name.rsplit('.', 1)[0])
        else:
            # otherwise create a new split-console logger
            logger.setLevel(logging.DEBUG)
            if console_level != logging.NOTSET:
                if console_level <= logging.INFO:
                    class LoggingOutputFilter(logging.Filter):
                        def filter(self, rec):
                            return rec.levelno in (logging.DEBUG, logging.INFO)

                    console_output = logging.StreamHandler(sys.stdout)
                    console_output.setLevel(console_level)
                    console_output.addFilter(LoggingOutputFilter())
                    logger.addHandler(console_output)

                console_errors = logging.StreamHandler(sys.stderr)
                console_errors.setLevel(max((console_level, logging.WARNING)))
                logger.addHandler(console_errors)

    if log_filename is not None:
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(file_level)
        for existing_file_handler in [
            handler for handler in logger.handlers if type(handler) is logging.FileHandler
        ]:
            logger.removeHandler(existing_file_handler)
        logger.addHandler(file_handler)

    if log_format is None:
        log_format = '[%(asctime)s] %(name)-9s %(levelname)-8s: %(message)s'
    log_formatter = logging.Formatter(log_format)
    for handler in logger.handlers:
        handler.setFormatter(log_formatter)

    return logger


LOGGER = get_logger('cplmdldrv')


def create_symlink(
    source_filename: PathLike, symlink_filename: PathLike, relative: bool = False
):
    if not isinstance(source_filename, Path):
        source_filename = Path(source_filename)
    if not isinstance(symlink_filename, Path):
        symlink_filename = Path(symlink_filename)

    if symlink_filename.is_symlink():
        LOGGER.debug(f'removing symlink {symlink_filename}')
        os.remove(symlink_filename)
    symlink_filename = symlink_filename.parent.absolute().resolve() / symlink_filename.name

    starting_directory = None
    if relative:
        starting_directory = Path().cwd().resolve()
        os.chdir(symlink_filename.parent)
        if os.path.isabs(source_filename):
            try:
                source_filename = Path(
                    os.path.relpath(source_filename, symlink_filename.parent)
                )
            except ValueError as error:
                LOGGER.warning(error)
                os.chdir(starting_directory)
    else:
        source_filename = source_filename.absolute()

    try:
        symlink_filename.symlink_to(source_filename)
    except Exception as error:
        LOGGER.warning(f'could not create symbolic link: {error}')
        shutil.copyfile(source_filename, symlink_filename)
    finally:
        if starting_directory is not None:
            os.chdir(starting_directory)


def ellipsoidal_distance(
    point_a: (float, float), point_b: (float, float), crs_a: CRS, crs_b: CRS = None
) -> float:
    if isinstance(point_a, Point):
        point_a = [*point_a.coords]
    if isinstance(point_b, Point):
        point_b = [*point_b.coords]
    if crs_b is not None:
        transformer = Transformer.from_crs(crs_b, crs_a)
        point_b = transformer.transform(*point_b)
    points = numpy.stack((point_a, point_b), axis=0)
    ellipsoid = crs_a.datum.to_json_dict()['ellipsoid']
    geodetic = Geod(a=ellipsoid['semi_major_axis'], rf=ellipsoid['inverse_flattening'])
    return geodetic.line_length(points[:, 0], points[:, 1])


def make_executable(path: PathLike):
    """ https://stackoverflow.com/questions/12791997/how-do-you-do-a-simple-chmod-x-from-within-python """
    mode = os.stat(path).st_mode
    mode |= (mode & 0o444) >> 2  # copy R bits to X
    os.chmod(path, mode)


def convert_value(value: Any, to_type: type) -> Any:
    if to_type is None:
        return value
    elif isinstance(to_type, str):
        to_type = eval(to_type)
    if isinstance(value, Enum):
        value = value.name
    if isinstance(to_type, Collection):
        collection_type = type(to_type)
        if collection_type is not EnumMeta:
            if not issubclass(collection_type, Mapping):
                if value is not None:
                    to_type = list(to_type)
                    if not isinstance(value, Iterable) or isinstance(value, str):
                        value = [value]
                    if len(to_type) == 1:
                        to_type = [to_type[0] for _ in value]
                    elif len(to_type) == len(value):
                        to_type = to_type[: len(value)]
                    else:
                        raise ValueError(
                            f'unable to convert list of values of length {len(value)} '
                            f'to list of types of length {len(to_type)}: '
                            f'{value} -/> {to_type}'
                        )
                    value = collection_type(
                        convert_value(value[index], current_type)
                        for index, current_type in enumerate(to_type)
                    )
                else:
                    value = collection_type()
            elif isinstance(value, str):
                value = json.loads(value)
            elif isinstance(value, CRS):
                value = value.to_json_dict()
        elif value is not None:
            try:
                value = to_type[value]
            except (KeyError, ValueError):
                try:
                    value = to_type(value)
                except (KeyError, ValueError):
                    raise ValueError(
                        f'unrecognized entry "{value}"; must be one of {list(to_type)}'
                    )
    elif not isinstance(value, to_type) and value is not None:
        if isinstance(value, timedelta):
            if issubclass(to_type, str):
                hours, remainder = divmod(value, timedelta(hours=1))
                minutes, remainder = divmod(remainder, timedelta(minutes=1))
                seconds = remainder / timedelta(seconds=1)
                value = f'{hours:02}:{minutes:02}:{seconds:04.3}'
            else:
                value /= timedelta(seconds=1)
        elif isinstance(value, CRS):
            if issubclass(to_type, str):
                value = value.to_wkt()
            elif issubclass(to_type, dict):
                value = value.to_json_dict()
            elif issubclass(to_type, int):
                value = value.to_epsg()
        if issubclass(to_type, bool):
            value = eval(f'{value}')
        elif issubclass(to_type, datetime):
            value = parse_date(value)
        elif issubclass(to_type, timedelta):
            try:
                try:
                    time = datetime.strptime(value, '%H:%M:%S')
                    value = timedelta(
                        hours=time.hour, minutes=time.minute, seconds=time.second
                    )
                except:
                    parts = [float(part) for part in value.split(':')]
                    if len(parts) > 3:
                        days = parts.pop(0)
                    else:
                        days = 0
                    value = timedelta(
                        days=days, hours=parts[0], minutes=parts[1], seconds=parts[2]
                    )
            except:
                value = timedelta(seconds=float(value))
        elif isinstance(value, str):
            try:
                value = to_type.from_string(value)
            except:
                value = to_type(value)
        else:
            value = to_type(value)
    return value


def convert_to_json(value: Any) -> Union[str, float, int, dict, list, bool]:
    if isinstance(value, Path):
        value = value.as_posix()
    elif isinstance(value, Enum):
        value = value.name
    if type(value) not in (float, int, bool, str):
        if isinstance(value, Collection) and not isinstance(value, str):
            if isinstance(value, Mapping):
                value = {
                    convert_to_json(key): convert_to_json(entry)
                    for key, entry in value.items()
                }
            else:
                value = [convert_to_json(entry) for entry in value]
        else:
            try:
                value = convert_value(value, float)
            except:
                try:
                    value = convert_value(value, int)
                except:
                    try:
                        value = convert_value(value, bool)
                    except:
                        value = convert_value(value, str)
    return value
