from abc import ABC
from os import PathLike
from pathlib import Path
from typing import Collection, Mapping, Union

from adcircpy.forcing.base import Forcing
from nemspy.model import ModelEntry

from coupledmodeldriver.configure.forcings.base import ForcingJSON
from .base import ConfigurationJSON, NEMSCapJSON


class RunConfiguration(ABC):
    required: [ConfigurationJSON] = []
    forcings: [ForcingJSON] = []

    def __init__(self, configurations: [ConfigurationJSON]):
        self.__configurations = {}
        self.configurations = configurations

    @property
    def configurations(self) -> {str: ConfigurationJSON}:
        return self.__configurations

    @configurations.setter
    def configurations(self, configurations: {str: ConfigurationJSON}):
        if isinstance(configurations, Collection) and not isinstance(configurations, Mapping):
            configurations = {entry.name: entry for entry in configurations}
        for name, configuration in configurations.items():
            self[name] = configuration

    @property
    def nemspy_entries(self) -> [ModelEntry]:
        return [
            configuration.nemspy_entry
            for configuration in self.configurations.values()
            if isinstance(configuration, NEMSCapJSON)
        ]

    def __contains__(self, configuration: Union[str, ConfigurationJSON]) -> bool:
        if isinstance(configuration, ConfigurationJSON):
            configuration = configuration.name
        return configuration in self.configurations

    def __getitem__(self, name: str) -> ConfigurationJSON:
        return self.configurations[name]

    def __setitem__(self, name: str, value: ConfigurationJSON):
        if isinstance(value, str):
            if Path(value).exists():
                value = ConfigurationJSON.from_file(value)
            else:
                value = ConfigurationJSON.from_string(value)
        elif isinstance(value, Forcing):
            value = ForcingJSON.from_adcircpy(value)
        self.configurations[name] = value

    @classmethod
    def read_directory(cls, directory: PathLike) -> 'RunConfiguration':
        if not isinstance(directory, Path):
            directory = Path(directory)
        if directory.is_file():
            directory = directory.parent

        configurations = []
        for configuration_class in cls.required:
            filename = directory / configuration_class.default_filename
            if filename.exists():
                configurations.append(configuration_class.from_file(filename))
            else:
                raise FileNotFoundError(f'missing required configuration file "{filename}"')

        for configuration_class in cls.forcings:
            filename = directory / configuration_class.default_filename
            if filename.exists():
                configurations.append(configuration_class.from_file(filename))

        return cls(configurations)

    def write_directory(self, directory: PathLike, overwrite: bool = False):
        """
        :param directory: directory in which to write generated JSON configuration files
        :param overwrite: whether to overwrite existing files
        """

        if not isinstance(directory, Path):
            directory = Path(directory)

        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)

        for configuration in self.__configurations.values():
            configuration.to_file(directory, overwrite=overwrite)
