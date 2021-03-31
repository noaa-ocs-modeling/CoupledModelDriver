from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
import json
from os import PathLike
from pathlib import Path
from typing import Any, Collection, Mapping, Union

from adcircpy import AdcircMesh, AdcircRun
from adcircpy.forcing.base import Forcing
from adcircpy.forcing.tides.tides import TidalSource, Tides
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing
from adcircpy.server import SlurmConfig
from nemspy import ModelingSystem
from nemspy.model import ADCIRCEntry, AtmosphericMeshEntry, ModelEntry, \
    WaveMeshEntry

from .job_script import SlurmEmailType
from .platforms import Platform
from .utilities import LOGGER, convert_to_json, convert_value


class Model(Enum):
    ADCIRC = 'ADCIRC'
    TidalForcing = 'Tides'
    ATMESH = 'ATMESH'
    WW3DATA = 'WW3DATA'


class GWCESolutionScheme(Enum):
    explicit = 'explicit'
    semi_implicit = 'semi-implicit'
    semi_implicit_legacy = 'semi-implicit-legacy'


class ConfigurationJSON(ABC):
    name: PathLike = None
    default_filename = f'configure.json'
    field_types: {str: type} = {}

    def __init__(self, fields: {str: type} = None, configuration: {str: Any} = None):
        if fields is None:
            fields = {}

        fields.update(self.field_types)

        if configuration is None:
            configuration = {field: None for field in fields}

        self.__fields = fields
        self.__configuration = configuration

    @property
    def fields(self) -> {str: type}:
        return self.__fields

    @property
    def configuration(self) -> {str: Any}:
        return self.__configuration

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
                f'adding new configuration entry "{key}: {field_type}" with value "{value}"'
            )
        self.__configuration[key] = convert_value(value, field_type)
        if key not in self.fields:
            self.__fields[key] = field_type

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

        return json.dumps(configuration)

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
                json.dump(configuration, file)
        else:
            LOGGER.debug(f'skipping existing file "{filename}"')


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
    ):
        super().__init__()

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


class NEMSJSON(ConfigurationJSON):
    name = 'nems'
    default_filename = f'configure_nems.json'
    field_types = {
        'executable_path': Path,
        'modeled_start_time': datetime,
        'modeled_end_time': datetime,
        'interval': timedelta,
        'models': [ModelEntry],
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
        models: [ModelEntry] = None,
        connections: [[str]] = None,
        mediations: [[str]] = None,
        sequence: [str] = None,
    ):
        super().__init__()

        self['executable_path'] = executable_path
        self['modeled_start_time'] = modeled_start_time
        self['modeled_end_time'] = modeled_end_time
        self['interval'] = interval
        self['models'] = models
        self['connections'] = connections
        self['mediations'] = mediations
        self['sequence'] = sequence

    @property
    def nemspy_modeling_system(self) -> ModelingSystem:
        modeling_system = ModelingSystem(
            start_time=self['modeled_start_time'],
            end_time=self['modeled_end_time'],
            interval=self['interval'],
            **{model.model_type.value.lower(): model for model in self['models']},
        )
        for connection in self['connections']:
            modeling_system.connect(*connection)
        for mediation in self['mediations']:
            modeling_system.mediate(*mediation)

        modeling_system.sequence = self['sequence']

        return modeling_system

    def to_nemspy(self) -> ModelingSystem:
        return self.nemspy_modeling_system

    @classmethod
    def from_nemspy(cls, modeling_system: ModelingSystem, executable_path: PathLike = None):
        if executable_path is None:
            executable_path = 'NEMS.x'
        return cls(
            executable_path=executable_path,
            modeled_start_time=modeling_system.start_time,
            modeled_end_time=modeling_system.end_time,
            modeled_timestep=modeling_system.interval,
            models=modeling_system.models,
            connections=modeling_system.connections,
            sequence=modeling_system.sequence,
        )


class ModelJSON(ConfigurationJSON, ABC):
    def __init__(self, model: Model, fields: {str: type} = None):
        if not isinstance(model, Model):
            model = Model[str(model).lower()]
        if fields is None:
            fields = {}

        fields.update(self.field_types)

        self.model = model
        ConfigurationJSON.__init__(self, fields=fields)


class NEMSCapJSON(ConfigurationJSON, ABC):
    field_types = {
        'processors': int,
        'nems_parameters': {str: str},
    }

    def __init__(self, processors: int, nems_parameters: {str: str} = None):
        super().__init__(fields=self.fields, configuration=self.configuration)
        self.fields.update(NEMSCapJSON.field_types)

        if nems_parameters is None:
            nems_parameters = {}

        self['processors'] = processors
        self['nems_parameters'] = nems_parameters

    @abstractmethod
    def nemspy_entry(self) -> ModelEntry:
        raise NotImplementedError()


class ADCIRCJSON(ModelJSON, NEMSCapJSON):
    name = 'adcirc'
    default_filename = f'configure_adcirc.json'
    field_types = {
        'adcprep_executable_path': Path,
        'modeled_start_time': datetime,
        'modeled_end_time': datetime,
        'modeled_timestep': timedelta,
        'fort_13_path': Path,
        'fort_14_path': Path,
        'tidal_spinup_duration': timedelta,
        'tidal_spinup_timestep': timedelta,
        'gwce_solution_scheme': GWCESolutionScheme,
        'use_smagorinsky': bool,
        'source_filename': Path,
        'use_original_mesh': bool,
        'stations_file_path': Path,
        'write_surface_output': bool,
        'write_station_output': bool,
    }

    def __init__(
        self,
        adcprep_executable_path: PathLike,
        modeled_start_time: datetime,
        modeled_end_time: datetime,
        modeled_timestep: timedelta,
        fort_13_path: PathLike,
        fort_14_path: PathLike,
        tidal_spinup_duration: timedelta = None,
        tidal_spinup_timestep: timedelta = None,
        forcings: [Forcing] = None,
        gwce_solution_scheme: str = None,
        use_smagorinsky: bool = None,
        source_filename: PathLike = None,
        slurm_configuration: SlurmJSON = None,
        use_original_mesh: bool = False,
        stations_file_path: PathLike = None,
        write_surface_output: bool = True,
        write_station_output: bool = False,
        processors: int = 11,
        nems_parameters: {str: str} = None,
    ):
        """

        :param adcprep_executable_path: file path to `adcprep`
        :param modeled_start_time: start time in model run
        :param modeled_end_time: edn time in model run
        :param modeled_timestep: time interval between model steps
        :param fort_13_path: file path to `fort.13`
        :param fort_14_path: file path to `fort.14`
        :param fort_14_path: file path to `fort.14`
        :param tidal_spinup_duration: tidal spinup duration for ADCIRC coldstart
        :param tidal_spinup_timestep: tidal spinup modeled time interval for ADCIRC coldstart
        :param forcings: list of Forcing objects to apply to the mesh
        :param gwce_solution_scheme: solution scheme (can be `explicit`, `semi-implicit`, or `semi-implicit-legacy`)
        :param use_smagorinsky: whether to use Smagorinsky coefficient
        :param source_filename: path to modulefile to `source`
        :param slurm_configuration: Slurm configuration object
        :param use_original_mesh: whether to symlink / copy original mesh instead of rewriting with `adcircpy`
        :param stations_file_path: file path to stations file
        :param write_surface_output: whether to write surface output to NetCDF
        :param write_station_output: whether to write station output to NetCDF (only applicable if stations file exists)
        :param processors: number of processors to use
        :param nems_parameters: parameters to give to NEMS cap
        """

        if tidal_spinup_timestep is None:
            tidal_spinup_timestep = modeled_timestep

        if forcings is None:
            forcings = []

        ModelJSON.__init__(self, model=Model.ADCIRC)
        NEMSCapJSON.__init__(self, processors=processors, nems_parameters=nems_parameters)

        self['adcprep_executable_path'] = adcprep_executable_path
        self['modeled_start_time'] = modeled_start_time
        self['modeled_end_time'] = modeled_end_time
        self['modeled_timestep'] = modeled_timestep
        self['fort_13_path'] = fort_13_path
        self['fort_14_path'] = fort_14_path
        self['tidal_spinup_duration'] = tidal_spinup_duration
        self['tidal_spinup_timestep'] = tidal_spinup_timestep
        self['gwce_solution_scheme'] = gwce_solution_scheme
        self['use_smagorinsky'] = use_smagorinsky
        self['source_filename'] = source_filename
        self['use_original_mesh'] = use_original_mesh
        self['stations_file_path'] = stations_file_path
        self['write_surface_output'] = write_surface_output
        self['write_station_output'] = write_station_output

        self.forcings = forcings
        self.slurm_configuration = slurm_configuration

    @property
    def forcings(self) -> ['ForcingJSON']:
        return self.__forcings

    @forcings.setter
    def forcings(self, forcings: ['ForcingJSON']):
        for index, forcing in enumerate(forcings):
            if isinstance(forcing, Forcing):
                forcings[index] = ForcingJSON.from_adcircpy(forcing)
        self.__forcings = forcings

    @property
    def slurm_configuration(self) -> [SlurmJSON]:
        return self.__slurm_configuration

    @slurm_configuration.setter
    def slurm_configuration(self, slurm_configuration: SlurmJSON):
        if isinstance(slurm_configuration, SlurmConfig):
            SlurmJSON.from_adcircpy(slurm_configuration)
        self.__slurm_configuration = slurm_configuration

    @property
    def adcircpy_mesh(self) -> AdcircMesh:
        LOGGER.info(f'opening mesh "{self["fort_14_path"]}"')
        mesh = AdcircMesh.open(self['fort_14_path'], crs=4326)

        LOGGER.debug(f'adding {len(self.forcings)} forcing(s) to mesh')
        for forcing in self.forcings:
            mesh.add_forcing(forcing.adcircpy_forcing)

        if self['fort_13_path'] is not None:
            LOGGER.info(f'reading attributes from "{self["fort_13_path"]}"')
            if self['fort_13_path'].exists():
                mesh.import_nodal_attributes(self['fort_13_path'])
                for attribute_name in mesh.get_nodal_attribute_names():
                    mesh.set_nodal_attribute_state(
                        attribute_name, coldstart=True, hotstart=True
                    )
            else:
                LOGGER.warning(
                    'mesh values (nodal attributes) not found ' f'at "{self["fort_13_path"]}"'
                )

        if not mesh.has_nodal_attribute('primitive_weighting_in_continuity_equation'):
            LOGGER.debug(f'generating tau0 in mesh')
            mesh.generate_tau0()

        return mesh

    @property
    def adcircpy_driver(self) -> AdcircRun:
        # instantiate AdcircRun object.
        driver = AdcircRun(
            mesh=self.adcircpy_mesh,
            start_date=self['modeled_start_time'],
            end_date=self['modeled_end_time'],
            spinup_time=self['tidal_spinup_duration'],
            server_config=self.slurm_configuration.to_adcircpy()
            if self.slurm_configuration is not None
            else None,
        )

        if self['modeled_timestep'] is not None:
            driver.timestep = self['modeled_timestep'] / timedelta(seconds=1)

        if self['gwce_solution_scheme'] is not None:
            driver.gwce_solution_scheme = self['gwce_solution_scheme'].value

        if self['use_smagorinsky'] is not None:
            driver.smagorinsky = self['use_smagorinsky']

        # spinup_start = self['modeled_start_time'] - self['tidal_spinup_duration']
        # spinup_end = self['modeled_start_time']

        if self['write_station_output'] and self['stations_file_path'].exists():
            driver.import_stations(self['stations_file_path'])
            driver.set_elevation_stations_output(
                self['modeled_timestep'], spinup=self['tidal_spinup_timestep']
            )
            # spinup_start=spinup_start, spinup_end=spinup_end)
            driver.set_velocity_stations_output(
                self['modeled_timestep'], spinup=self['tidal_spinup_timestep']
            )
            # spinup_start=spinup_start, spinup_end=spinup_end)

        if self['write_surface_output']:
            driver.set_elevation_surface_output(
                self['modeled_timestep'], spinup=self['tidal_spinup_timestep']
            )
            # spinup_start=spinup_start, spinup_end=spinup_end)
            driver.set_velocity_surface_output(
                self['modeled_timestep'], spinup=self['tidal_spinup_timestep']
            )
            # spinup_start=spinup_start, spinup_end=spinup_end)

        return driver

    @property
    def nemspy_entry(self) -> ADCIRCEntry:
        return ADCIRCEntry(processors=self['processors'], **self['nems_parameters'])


class ForcingJSON(ConfigurationJSON, ABC):
    field_types = {'resource': str}

    def __init__(self, resource: PathLike, fields: {str: type} = None):
        if fields is None:
            fields = {}

        fields.update(self.field_types)
        fields.update(ForcingJSON.field_types)

        ConfigurationJSON.__init__(self, fields=fields)
        self['resource'] = resource

    @property
    @abstractmethod
    def adcircpy_forcing(self) -> Forcing:
        raise NotImplementedError

    def to_adcircpy(self) -> Forcing:
        return self.adcircpy_forcing

    @classmethod
    @abstractmethod
    def from_adcircpy(cls, forcing: Forcing) -> 'ForcingJSON':
        raise NotImplementedError()


class TidalForcingJSON(ForcingJSON):
    name = 'tidal_forcing'
    default_filename = f'configure_tidal_forcing.json'
    field_types = {'tidal_source': TidalSource, 'constituents': [str]}

    def __init__(
        self,
        resource: PathLike = None,
        tidal_source: TidalSource = TidalSource.TPXO,
        constituents: [str] = None,
    ):
        if constituents is None:
            constituents = 'ALL'
        elif not isinstance(constituents, str):
            constituents = list(constituents)

        super().__init__(resource=resource)

        self['tidal_source'] = tidal_source
        self['constituents'] = constituents

    @property
    def adcircpy_forcing(self) -> Forcing:
        tides = Tides(tidal_source=self['tidal_source'], resource=self['resource'])

        constituents = [constituent.capitalize() for constituent in self['constituents']]

        if sorted(constituents) == sorted(
            constituent.capitalize() for constituent in tides.constituents
        ):
            constituents = ['All']
        elif sorted(constituents) == sorted(
            constituent.capitalize() for constituent in tides.major_constituents
        ):
            constituents = ['Major']

        if 'All' in constituents:
            tides.use_all()
        elif 'Major' in constituents:
            tides.use_major()
        else:
            for constituent in constituents:
                tides.use_constituent(constituent)

        self['constituents'] = list(tides.active_constituents)
        return tides

    @classmethod
    def from_adcircpy(cls, forcing: Tides) -> 'TidalForcingJSON':
        return cls(
            resource=forcing.tidal_dataset.path,
            tidal_source=forcing.tidal_source,
            constituents=forcing.active_constituents,
        )


class ATMESHForcingJSON(ForcingJSON, NEMSCapJSON):
    name = 'atmesh'
    default_filename = f'configure_atmesh.json'
    field_types = {
        'nws': int,
        'modeled_timestep': timedelta,
    }

    def __init__(
        self,
        resource: PathLike,
        nws: int = 17,
        modeled_timestep: timedelta = timedelta(hours=1),
        processors: int = 1,
        nems_parameters: {str: str} = None,
    ):
        ForcingJSON.__init__(self, resource=resource)
        NEMSCapJSON.__init__(self, processors=processors, nems_parameters=nems_parameters)

        self['nws'] = nws
        self['modeled_timestep'] = modeled_timestep

    @property
    def adcircpy_forcing(self) -> Forcing:
        return AtmosphericMeshForcing(
            filename=self['resource'],
            nws=self['nws'],
            interval_seconds=self['modeled_timestep'] / timedelta(seconds=1),
        )

    @classmethod
    def from_adcircpy(cls, forcing: AtmosphericMeshForcing) -> 'ATMESHForcingJSON':
        return cls(
            resource=forcing.filename, nws=forcing.NWS, modeled_timestep=forcing.interval,
        )

    @property
    def nemspy_entry(self) -> AtmosphericMeshEntry:
        return AtmosphericMeshEntry(
            filename=self['resource'], processors=self['processors'], **self['nems_parameters']
        )


class WW3DATAForcingJSON(ForcingJSON, NEMSCapJSON):
    name = 'ww3data'
    default_filename = f'configure_ww3data.json'
    field_types = {'nrs': int, 'modeled_timestep': timedelta}

    def __init__(
        self,
        resource: PathLike,
        nrs: int = 5,
        modeled_timestep: timedelta = timedelta(hours=1),
        processors: int = 1,
        nems_parameters: {str: str} = None,
    ):
        ForcingJSON.__init__(self, resource=resource)
        NEMSCapJSON.__init__(self, processors=processors, nems_parameters=nems_parameters)

        self['nrs'] = nrs
        self['modeled_timestep'] = modeled_timestep

    @property
    def adcircpy_forcing(self) -> Forcing:
        return WaveWatch3DataForcing(
            filename=self['resource'],
            nrs=self['nrs'],
            interval_seconds=self['modeled_timestep'],
        )

    @classmethod
    def from_adcircpy(cls, forcing: WaveWatch3DataForcing) -> 'WW3DATAForcingJSON':
        return cls(
            resource=forcing.filename, nrs=forcing.NRS, modeled_timestep=forcing.interval,
        )

    @property
    def nemspy_entry(self) -> WaveMeshEntry:
        return WaveMeshEntry(
            filename=self['resource'], processors=self['processors'], **self['nems_parameters']
        )


class ModelDriverJSON(ConfigurationJSON):
    name = 'modeldriver'
    default_filename = f'configure_modeldriver.json'
    field_types = {
        'platform': Platform,
        'runs': {str: (str, Any)},
    }

    def __init__(
        self, platform: Platform, runs: {str: (str, Any)} = None,
    ):
        if runs is None:
            runs = {'run_1': (None, None)}

        super().__init__()

        self['platform'] = platform
        self['runs'] = runs


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
        for name, configuration_class in cls.required:
            filename = directory / configuration_class.default_filename
            if filename.exists():
                configurations.append(configuration_class.from_file(filename))
            else:
                raise FileNotFoundError(f'missing required configuration file "{filename}"')

        for name, configuration_class in cls.forcings:
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
