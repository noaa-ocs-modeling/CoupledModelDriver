from abc import ABC, abstractmethod
from copy import copy
from datetime import datetime, timedelta
import json
import os
from os import PathLike
from pathlib import Path, PurePosixPath
from typing import Any, Union

from adcircpy.server import SlurmConfig
from nemspy import ModelingSystem
from nemspy.model.base import ModelEntry

from coupledmodeldriver.platforms import Platform
from coupledmodeldriver.script import SlurmEmailType
from coupledmodeldriver.utilities import convert_to_json, convert_value, LOGGER


class ConfigurationJSON(ABC):
    name: str
    default_filename: PathLike
    field_types: {str: type}

    def __init__(self, fields: {str: type} = None, **configuration):
        self.field_types = {key.lower(): value for key, value in self.field_types.items()}

        if not hasattr(self, 'fields'):
            self.fields = {}

        self.fields.update({key: None for key in self.field_types if key not in self.fields})

        if fields is not None:
            fields = {key.lower(): value for key, value in fields.items()}
            self.fields.update(fields)

        if not hasattr(self, 'configuration'):
            self.configuration = {field: None for field in self.fields}

        if len(configuration) > 0:
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

    def move_paths(self, relative: PathLike):
        """
        :param relative: path to which to move Path attributes
        """

        for name, value in self.configuration.items():
            if isinstance(value, Path) and not value.is_absolute():
                self[name] = PurePosixPath(move_path(value, relative).resolve())

    def relative_to(self, path: PathLike, inplace: bool = False) -> 'ConfigurationJSON':
        instance = copy(self) if not inplace else self
        for name, value in instance.configuration.items():
            if isinstance(value, Path):
                instance[name] = PurePosixPath(os.path.relpath(value.resolve(), path))
        return instance

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
                f'adding new configuration entry "{key}: {field_type}" to {self.name}"'
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

    def __copy__(self) -> 'ConfigurationJSON':
        return self.__class__(**self.configuration)

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
            LOGGER.debug(f'reading file "{os.path.relpath(filename.resolve(), Path.cwd())}"')
            try:
                configuration = json.load(file)
            except Exception as error:
                raise SyntaxError(f'{error} in file "{filename}"')

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

        for key, value in self.configuration.items():
            if isinstance(value, Path) and value.is_absolute():
                try:
                    value = Path(os.path.relpath(value, filename.absolute().parent))
                except:
                    pass
                self.configuration[key] = value

        configuration = convert_to_json(self.configuration)

        if any(key != key.lower() for key in configuration):
            configuration = {key.lower(): value for key, value in configuration.items()}

        if overwrite or not filename.exists():
            with open(filename.absolute(), 'w') as file:
                LOGGER.debug(
                    f'writing to file "{os.path.relpath(filename.resolve(), Path.cwd())}"'
                )
                json.dump(configuration, file, indent=2)
        else:
            LOGGER.debug(
                f'skipping existing file "{os.path.relpath(filename.resolve(), Path.cwd())}"'
            )


class SlurmJSON(ConfigurationJSON):
    name = 'Slurm'
    default_filename = f'configure_slurm.json'
    field_types = {
        'account': str,
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
        if tasks is None:
            tasks = 1

        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(SlurmJSON.field_types)

        ConfigurationJSON.__init__(self, **kwargs)

        self.tasks = tasks

        self['account'] = account
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
            ntasks=self.tasks,
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
    name = 'ModelDriver'
    default_filename = f'configure_modeldriver.json'
    field_types = {
        'platform': Platform,
        'perturbations': {str: {str: {str: Any}}},
    }

    def __init__(
        self, platform: Platform, perturbations: {str: {str: {str: Any}}} = None, **kwargs
    ):
        """
        :param platform: platform on which to run
        :param perturbations: dictionary of runs encompassing run names to parameter values
        """

        if perturbations is None:
            perturbations = {'unperturbed': None}

        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(ModelDriverJSON.field_types)

        ConfigurationJSON.__init__(self, **kwargs)

        self['platform'] = platform
        self['perturbations'] = perturbations


class AttributeJSON(ConfigurationJSON):
    default_attributes: [str]
    field_types = {
        'attributes': {str: Any},
    }

    def __init__(self, attributes: {str: Any} = None, **kwargs):
        """
        :param attributes: attributes to store
        """

        if attributes is None:
            if self.default_attributes is not None:
                attributes = {attribute: None for attribute in self.default_attributes}
            else:
                attributes = {}

        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(AttributeJSON.field_types)

        ConfigurationJSON.__init__(self, **kwargs)

        self['attributes'] = attributes


class NEMSCapJSON(ConfigurationJSON, ABC):
    default_processors: int
    field_types = {
        'processors': int,
        'nems_parameters': {str: str},
    }

    def __init__(self, processors: int = None, nems_parameters: {str: str} = None, **kwargs):
        if processors is None:
            processors = self.default_processors
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


class NEMSJSON(ConfigurationJSON):
    name = 'NEMS'
    default_filename = f'configure_nems.json'
    field_types = {
        'executable_path': Path,
        'modeled_start_time': datetime,
        'modeled_end_time': datetime,
        'interval': timedelta,
        'connections': [[str]],
        'mediations': [str],
        'sequence': [str],
    }

    def __init__(
        self,
        executable_path: PathLike,
        modeled_start_time: datetime,
        modeled_end_time: datetime,
        interval: timedelta = None,
        connections: [[str]] = None,
        mediations: [[str]] = None,
        sequence: [str] = None,
        **kwargs,
    ):
        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(NEMSJSON.field_types)

        ConfigurationJSON.__init__(self, **kwargs)

        self['executable_path'] = executable_path
        self['modeled_start_time'] = modeled_start_time
        self['modeled_end_time'] = modeled_end_time
        self['interval'] = interval
        self['connections'] = connections
        self['mediations'] = mediations
        self['sequence'] = sequence

    def to_nemspy(self, models: [NEMSCapJSON]) -> ModelingSystem:
        models = [
            model.nemspy_entry if isinstance(model, NEMSCapJSON) else model for model in models
        ]
        modeling_system = ModelingSystem(
            start_time=self['modeled_start_time'],
            end_time=self['modeled_end_time'],
            interval=self['interval'],
            **{model.model_type.value.lower(): model for model in models},
        )
        for connection in self['connections']:
            modeling_system.connect(*connection)
        for mediation in self['mediations']:
            modeling_system.mediate(*mediation)

        if len(self['sequence']) > 0:
            modeling_system.sequence = self['sequence']
        else:
            self['sequence'] = modeling_system.sequence

        return modeling_system

    @classmethod
    def from_nemspy(cls, modeling_system: ModelingSystem, executable_path: PathLike = None):
        if executable_path is None:
            executable_path = 'NEMS.x'
        return cls(
            executable_path=executable_path,
            modeled_start_time=modeling_system.start_time,
            modeled_end_time=modeling_system.end_time,
            interval=modeling_system.interval,
            models=modeling_system.models,
            connections=modeling_system.connections,
            sequence=modeling_system.sequence,
        )


def move_path(path: PathLike, move: Union[PathLike, int]) -> Path:
    if not isinstance(path, Path):
        path = Path(path)

    try:
        move = int(move)
        if move <= 0:
            move = Path('.') / ('../' * (-move))
    except:
        if not isinstance(move, Path):
            move = Path(move)

    if path.is_absolute():
        output_path = path
    elif isinstance(move, int):
        output_path = Path(*path.parts[move:])
    else:
        output_path = move / path

    return output_path
