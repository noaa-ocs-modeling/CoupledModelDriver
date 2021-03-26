from enum import Enum
import json
from os import PathLike
from pathlib import Path
from typing import Any


class ModelConfiguration:
    pass


class ADCIRCConfiguration(ModelConfiguration):
    def __init__(self):
        self.data = {

        }


class FileWrapperConfiguration(ModelConfiguration):
    def __init__(self, filename: PathLike):
        self.filename = filename


class Model(Enum):
    ADCIRC = ADCIRCConfiguration
    ATMESH = FileWrapperConfiguration
    WW3DATA = FileWrapperConfiguration


class CoupledModelConfigurationFile:
    def __init__(self, models: [Model]):
        for index, model in enumerate(models):
            if not isinstance(model, Model):
                models[index] = Model[model]

        self.__data = {
            'output_directory': Path,
            'models': models,
            'mesh_directory': Path,
        }

    def __getitem__(self, key) -> Any:
        return self.__data[key]

    @property
    def output_directory(self):
        pass

    def write(self, filename: PathLike, overwrite: bool = False):
        """
        Write script to file.

        :param filename: path to output file
        :param overwrite: whether to overwrite existing files
        """

        if not isinstance(filename, Path):
            filename = Path(filename)

        output = f'{self}\n'
        if overwrite or not filename.exists():
            with open(filename, 'w') as file:
                file.write(output)

    @classmethod
    def from_file(cls, filename: PathLike) -> 'CoupledModelConfigurationFile':
        if not isinstance(filename, Path):
            filename = Path(filename)

        with open(filename) as file:
            json.load(file)

        instance = cls()

        return instance
