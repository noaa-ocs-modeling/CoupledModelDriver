from abc import ABC
from copy import copy
from os import PathLike
from pathlib import Path
from typing import Any, Collection, Dict, List, Mapping, Set, Union

from coupledmodeldriver.configure.base import ConfigurationJSON, ModelDriverJSON
from coupledmodeldriver.configure.forcings.base import (
    ADCIRCPY_FORCING_CLASSES,
    PYSCHISM_FORCING_CLASSES,
    ForcingJSON,
)
from coupledmodeldriver.utilities import LOGGER


class RunConfiguration(ABC):
    """
    abstraction of a set of model run configurations, encapsulated in JSON files
    """

    REQUIRED: Set[ConfigurationJSON] = {ModelDriverJSON}
    SUPPLEMENTARY: Set[ConfigurationJSON] = set()

    def __init__(self, configurations: List[ConfigurationJSON]):
        self.__configurations = {}
        self.configurations = configurations

    def perturb(self, relative_to: PathLike = None) -> Dict[str, 'RunConfiguration']:
        perturbed_configurations = {}
        if 'modeldriver' in self:
            perturbations = self['modeldriver']['perturbations']

            for run, run_perturbations in perturbations.items():
                instance = copy(self) if relative_to is None else self.relative_to(relative_to)
                if run_perturbations is not None and len(run_perturbations) > 0:
                    for name, configuration_perturbations in run_perturbations.items():
                        if name in instance:
                            instance[name].update(configuration_perturbations)
                        else:
                            LOGGER.warning(f'configuration "{name}" not found to perturb')
                perturbed_configurations[run] = instance
        else:
            perturbed_configurations = {}

        return perturbed_configurations

    @property
    def configurations(self) -> List[ConfigurationJSON]:
        return list(self.__configurations.values())

    @configurations.setter
    def configurations(self, configurations: List[ConfigurationJSON]):
        if isinstance(configurations, Collection) and not isinstance(configurations, Mapping):
            configurations = {
                entry.name.lower(): entry
                for entry in configurations
                if isinstance(entry, ConfigurationJSON)
            }
        for name, configuration in configurations.items():
            self[name] = configuration

    def move_paths(self, relative: PathLike):
        for configuration in self.configurations:
            configuration.move_paths(relative)

    def relative_to(self, path: PathLike, inplace: bool = False) -> 'RunConfiguration':
        instance = copy(self) if not inplace else self
        for configuration in instance.configurations:
            configuration.relative_to(path, inplace=True)
        return instance

    def __contains__(self, configuration: Union[str, ConfigurationJSON]) -> bool:
        if isinstance(configuration, str):
            return configuration.lower() in self.__configurations
        else:
            if not isinstance(configuration, ConfigurationJSON):
                configuration = from_user_input(configuration)
            return configuration in self.__configurations.values()

    def __getitem__(self, name: str) -> ConfigurationJSON:
        return self.__configurations[name.lower()]

    def __setitem__(self, name: str, configuration: ConfigurationJSON):
        if not isinstance(configuration, ConfigurationJSON):
            configuration = from_user_input(configuration)
        self.__configurations[name.lower()] = configuration

    def __iter__(self) -> ConfigurationJSON:
        yield from self.__configurations

    def items(self) -> (str, ConfigurationJSON):
        yield from self.__configurations.items()

    def __copy__(self) -> 'RunConfiguration':
        return self.__class__([copy(configuration) for configuration in self.configurations])

    def add(self, configuration: ConfigurationJSON) -> str:
        if not isinstance(configuration, ConfigurationJSON):
            configuration = from_user_input(configuration)
        self.__configurations[configuration.name.lower()] = configuration
        return configuration.name.lower()

    @classmethod
    def read_directory(
        cls, directory: PathLike, required: List[type] = None, supplementary: List[type] = None
    ) -> 'RunConfiguration':
        if not isinstance(directory, Path):
            directory = Path(directory)
        if directory.is_file():
            directory = directory.parent
        if required is None:
            required = set()
        required.update(RunConfiguration.REQUIRED)
        if supplementary is None:
            supplementary = set()
        supplementary.update(RunConfiguration.SUPPLEMENTARY)

        configurations = []
        for configuration_class in required:
            filename = directory / configuration_class.default_filename
            if filename.exists():
                configurations.append(configuration_class.from_file(filename))
            else:
                raise FileNotFoundError(f'missing required configuration file "{filename}"')

        for configuration_class in supplementary:
            filename = directory / configuration_class.default_filename
            if filename.exists():
                configurations.append(configuration_class.from_file(filename))

        return cls.from_configurations(configurations)

    def write_directory(
        self, directory: PathLike, absolute: bool = False, overwrite: bool = False
    ):
        """
        :param directory: directory in which to write generated JSON configuration files
        :param absolute: whether to write absolute paths
        :param overwrite: whether to overwrite existing files
        """

        if not isinstance(directory, Path):
            directory = Path(directory)

        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)

        for configuration in self.__configurations.values():
            configuration.to_file(directory, absolute=absolute, overwrite=overwrite)

    @classmethod
    def from_configurations(
        cls, configurations: List[ConfigurationJSON]
    ) -> 'RunConfiguration':
        return cls(configurations)


def from_user_input(value: Any) -> ConfigurationJSON:
    """
    construct a configuration JSON object from a forcing object, a file, or a string

    :param value: value to parse; either an ADCIRCpy forcing object, file path, or a JSON string value
    :return: configuration JSON object
    """

    if isinstance(value, str):
        if Path(value).exists():
            value = ConfigurationJSON.from_file(value)
        else:
            value = ConfigurationJSON.from_string(value)
    elif isinstance(value, ADCIRCPY_FORCING_CLASSES):
        value = ForcingJSON.from_adcircpy(value)
    elif isinstance(value, PYSCHISM_FORCING_CLASSES):
        value = ForcingJSON.from_pyschism(value)
    return value
