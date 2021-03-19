import logging
import os
from os import PathLike
from pathlib import Path
import shutil
import sys

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
        log_format = '[%(asctime)s] %(name)-15s %(levelname)-8s: %(message)s'
    log_formatter = logging.Formatter(log_format)
    for handler in logger.handlers:
        handler.setFormatter(log_formatter)

    return logger


LOGGER = get_logger('utilities')


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
    source_filename = source_filename.absolute().resolve()

    starting_directory = None
    if relative:
        starting_directory = Path().cwd().resolve()
        os.chdir(source_filename.parent)
        try:
            source_filename = source_filename.relative_to(symlink_filename.parent)
        except ValueError as error:
            LOGGER.warning(error)

    try:
        symlink_filename.symlink_to(source_filename)
    except Exception as error:
        LOGGER.warning(f'could not create symbolic link: {error}')
        shutil.copyfile(source_filename, symlink_filename)

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
