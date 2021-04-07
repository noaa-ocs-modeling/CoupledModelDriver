from abc import ABC, abstractmethod
from datetime import timedelta
import json
from os import PathLike
from pathlib import Path
from typing import Any

from adcircpy.server import SlurmConfig
from nemspy.model import ModelEntry

from ..platforms import Platform
from ..script import SlurmEmailType
from ..utilities import LOGGER, convert_to_json, convert_value


class ConfigurationJSON(ABC):
    name: PathLike
    default_filename: str
    field_types: {str: type}

    def __init__(self, fields: {str: type} = None, configuration: {str: Any} = None):
        if not hasattr(self, 'fields'):
            self.fields = {}

        if fields is not None:
            self.fields.update(fields)

        if not hasattr(self, 'configuration'):
            self.configuration = {field: None for field in self.fields}

        if configuration is not None:
            self.configuration.update(configuration)

    def update(self, configuration: {str: Any}):
        for key, value in configuration.items():
            if key in self:
                converted_value = convert_value(value, self.fields[key])
                if self[key] != converted_value:
                    value = converted_value
                else:
                    return
            self[key] = value

    def update_from_file(self, filename: PathLike):
        with open(filename) as file:
            configuration = json.load(file)
        self.update(configuration)

    def __contains__(self, key: str) -> bool:
        return key in self.configuration

    def __getitem__(self, key: str) -> Any:
        return self.configuration[key]

    def __setitem__(self, key: str, value: Any):
        if key in self.fields:
            field_type = self.fields[key]
        else:
            field_type = type(value)
            LOGGER.info(
                f'adding new configuration entry "{key}: {field_type}"' f' to {self.name}"'
            )
        self.configuration[key] = convert_value(value, field_type)
        if key not in self.fields:
            self.fields[key] = field_type

    def __eq__(self, other: 'ConfigurationJSON') -> bool:
        return other.configuration == self.configuration

    def __repr__(self):
        configuration_string = ', '.join(
            [f'{key}={repr(value)}' for key, value in self.configuration.items()]
        )
        return f'{self.__class__.__name__}({configuration_string})'

    @classmethod
    def from_dict(cls, configuration: {str: Any}) -> 'ConfigurationJSON':
        return cls(**configuration)

    def to_dict(self) -> {str: Any}:
        return self.configuration

    @classmethod
    def from_string(cls, string: str) -> 'ConfigurationJSON':
        configuration = json.loads(string)

        configuration = {
            key.lower(): convert_value(value, cls.field_types[key])
            if key in cls.field_types
            else convert_to_json(value)
            for key, value in configuration.items()
        }

        return cls(**configuration)

    def __str__(self) -> str:
        configuration = convert_to_json(self.configuration)

        if any(key != key.lower() for key in configuration):
            configuration = {key.lower(): value for key, value in configuration.items()}

        return json.dumps(configuration, indent=2)

    @classmethod
    def from_file(cls, filename: PathLike) -> 'ConfigurationJSON':
        """
        create new object from an existing JSON file

        :param filename: path to JSON file
        :return: configuration object
        """

        if not isinstance(filename, Path):
            filename = Path(filename)

        if filename.is_dir():
            filename = filename / cls.default_filename

        with open(filename) as file:
            LOGGER.debug(f'reading file "{filename}"')
            configuration = json.load(file)

        configuration = {
            key.lower(): convert_value(value, cls.field_types[key])
            if key in cls.field_types
            else convert_to_json(value)
            for key, value in configuration.items()
        }

        return cls(**configuration)

    def to_file(self, filename: PathLike = None, overwrite: bool = False):
        """
        Write script to file.

        :param filename: path to output file
        :param overwrite: whether to overwrite existing file
        """

        if filename is None:
            filename = self['output_directory']
        elif not isinstance(filename, Path):
            filename = Path(filename)

        if filename.is_dir():
            filename = filename / self.default_filename

        if not filename.parent.exists():
            filename.mkdir(parents=True, exist_ok=True)

        configuration = convert_to_json(self.configuration)

        if any(key != key.lower() for key in configuration):
            configuration = {key.lower(): value for key, value in configuration.items()}

        if overwrite or not filename.exists():
            with open(filename.absolute(), 'w') as file:
                LOGGER.debug(f'writing to file "{filename}"')
                json.dump(configuration, file, indent=2)
        else:
            LOGGER.debug(f'skipping existing file "{filename}"')


class NEMSCapJSON(ConfigurationJSON, ABC):
    field_types = {
        'processors': int,
        'nems_parameters': {str: str},
    }

    def __init__(self, processors: int, nems_parameters: {str: str} = None, **kwargs):
        if nems_parameters is None:
            nems_parameters = {}
        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(NEMSCapJSON.field_types)

        ConfigurationJSON.__init__(self, **kwargs)

        self['processors'] = processors
        self['nems_parameters'] = nems_parameters

    @abstractmethod
    def nemspy_entry(self) -> ModelEntry:
        raise NotImplementedError()


class SlurmJSON(ConfigurationJSON):
    name = 'slurm'
    default_filename = f'configure_slurm.json'
    field_types = {
        'account': str,
        'tasks': int,
        'partition': str,
        'job_duration': timedelta,
        'run_directory': Path,
        'run_name': str,
        'email_type': SlurmEmailType,
        'email_address': str,
        'log_filename': Path,
        'modules': [str],
        'path_prefix': Path,
        'extra_commands': [str],
        'launcher': str,
        'nodes': int,
    }

    def __init__(
        self,
        account: str,
        tasks: int = None,
        partition: str = None,
        job_duration: timedelta = None,
        run_directory: PathLike = None,
        run_name: str = None,
        email_type: SlurmEmailType = None,
        email_address: str = None,
        log_filename: PathLike = None,
        modules: [str] = None,
        path_prefix: Path = None,
        extra_commands: [str] = None,
        launcher: str = None,
        nodes: int = None,
        **kwargs,
    ):
        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(SlurmJSON.field_types)

        ConfigurationJSON.__init__(self, **kwargs)

        self['account'] = account
        self['tasks'] = tasks
        self['partition'] = partition
        self['job_duration'] = job_duration
        self['run_directory'] = run_directory
        self['run_name'] = run_name
        self['email_type'] = email_type
        self['email_address'] = email_address
        self['log_filename'] = log_filename
        self['modules'] = modules
        self['path_prefix'] = path_prefix
        self['extra_commands'] = extra_commands
        self['launcher'] = launcher
        self['nodes'] = nodes

        if self['email_type'] is None:
            if self['email_address'] is not None:
                self['email_type'] = SlurmEmailType.ALL

    def to_adcircpy(self) -> SlurmConfig:
        return SlurmConfig(
            account=self['account'],
            ntasks=self['tasks'],
            partition=self['partition'],
            walltime=self['job_duration'],
            filename=self['filename'] if 'filename' in self else None,
            run_directory=self['run_directory'],
            run_name=self['run_name'],
            mail_type=self['email_type'],
            mail_user=self['email_address'],
            log_filename=self['log_filename'],
            modules=self['modules'],
            path_prefix=self['path_prefix'],
            extra_commands=self['extra_commands'],
            launcher=self['launcher'],
            nodes=self['nodes'],
        )

    @classmethod
    def from_adcircpy(cls, slurm_config: SlurmConfig):
        instance = cls(
            account=slurm_config._account,
            tasks=slurm_config._slurm_ntasks,
            partition=slurm_config._partition,
            job_duration=slurm_config._walltime,
            run_directory=slurm_config._run_directory,
            run_name=slurm_config._run_name,
            email_type=slurm_config._mail_type,
            email_address=slurm_config._mail_user,
            log_filename=slurm_config._log_filename,
            modules=slurm_config._modules,
            path_prefix=slurm_config._path_prefix,
            extra_commands=slurm_config._extra_commands,
            launcher=slurm_config._launcher,
            nodes=slurm_config._nodes,
        )

        instance['filename'] = slurm_config._filename


class ModelDriverJSON(ConfigurationJSON):
    name = 'modeldriver'
    default_filename = f'configure_modeldriver.json'
    field_types = {
        'platform': Platform,
        'runs': {str: {str: Any}},
    }

    def __init__(self, platform: Platform, runs: {str: {str: Any}} = None, **kwargs):
        if runs is None:
            runs = {'run_1': None}
        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(ModelDriverJSON.field_types)

        ConfigurationJSON.__init__(self, **kwargs)

        self['platform'] = platform
        self['runs'] = runs
