from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from enum import Enum
import json
from os import PathLike
from pathlib import Path
from typing import Any

from adcircpy import AdcircMesh, AdcircRun
from adcircpy.forcing.base import Forcing
from adcircpy.forcing.tides.tides import TidalSource, Tides
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing
from adcircpy.server import SlurmConfig
from nemspy import ModelingSystem
from nemspy.model import ModelEntry

from coupledmodeldriver.platforms import Platform
from coupledmodeldriver.utilities import convert_value, get_logger

LOGGER = get_logger('configuration')


class Model(Enum):
    ADCIRC = 'ADCIRC'
    Tides = 'Tides'
    ATMesh = 'ATMesh'
    WW3Data = 'WW3Data'


class GWCESolutionScheme(Enum):
    explicit = 'explicit'
    semi_implicit = 'semi-implicit'
    semi_implicit_legacy = 'semi-implicit-legacy'


class Configuration(ABC):
    name: str

    def __init__(self, fields: {str: type}):
        if fields is None:
            fields = {}
        self.__fields = fields
        self.__configuration = {field: None for field in fields}

    @property
    def fields(self) -> {str: type}:
        return self.__fields

    @property
    def configuration(self) -> {str: Any}:
        return self.__configuration

    def write(self, filename: PathLike = None, overwrite: bool = False):
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
            filename = filename / f'configure_{self.name.lower()}.json'

        if overwrite or not filename.exists():
            with open(filename, 'w') as file:
                json.dump(self.configuration, file)
        else:
            raise FileExistsError(f'file exists at {filename}')

    def __getitem__(self, key: str) -> Any:
        return self.configuration[key]

    def __setitem__(self, key: str, value: Any):
        if key in self.fields:
            field_type = self.fields[key]
        else:
            field_type = type(value)
        self.__configuration[key] = convert_value(value, self.fields[key])
        self.__fields[key] = field_type

    def __str__(self) -> str:
        return json.dumps(self.configuration)

    def __repr__(self):
        configuration_string = ', '.join([f'{key}={repr(value)}'
                                          for key, value in self.configuration.items()])
        return f'{self.__class__.__name__}({configuration_string})'

    @classmethod
    def from_dict(cls, configuration: {str: Any}) -> 'Configuration':
        return cls(**configuration)

    @classmethod
    def from_file(cls, filename: PathLike) -> 'Configuration':
        if not isinstance(filename, Path):
            filename = Path(filename)

        with open(filename) as file:
            configuration = json.load(file)

        return cls(**configuration)


class SlurmConfiguration(Configuration):
    name = 'Slurm'

    def __init__(
        self,
        account: str,
        tasks: int = None,
        partition: str = None,
        job_duration: timedelta = None,
        run_directory: PathLike = None,
        run_name: str = None,
        email_type: str = None,
        email_address: str = None,
        log_filename: PathLike = None,
        modules: [str] = None,
        path_prefix: Path = None,
        extra_commands: [str] = None,
        launcher: str = None,
        nodes: int = None,
    ):
        super().__init__(fields={
            'account': str,
            'tasks': int,
            'partition': str,
            'job_duration': timedelta,
            'run_directory': Path,
            'run_name': str,
            'email_type': str,
            'email_address': str,
            'log_filename': Path,
            'modules': [str],
            'path_prefix': Path,
            'extra_commands': [str],
            'launcher': str,
            'nodes': int,
        })

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

    @property
    def slurm_configuration(self) -> SlurmConfig:
        return SlurmConfig(
            account=self['account'],
            ntasks=self['tasks'],
            partition=self['partition'],
            walltime=self['job_duration'],
            filename=None,
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


class NEMSConfiguration(Configuration):
    name = 'NEMS'

    def __init__(
        self,
        executable_path: PathLike,
        modeled_start_time: datetime,
        modeled_end_time: datetime,
        modeled_timestep: timedelta = None,
        models: [ModelEntry] = None,
        connections: [[str]] = None,
        mediations: [[str]] = None,
        sequence: [str] = None,
    ):
        super().__init__(fields={
            'executable_path': Path,
            'modeled_start_time': datetime,
            'modeled_end_time': datetime,
            'modeled_timestep': timedelta,
            'models': [ModelEntry],
            'connections': [[str]],
            'mediations': [str],
            'sequence': [str],
        })

        self['executable_path'] = executable_path
        self['modeled_start_time'] = modeled_start_time
        self['modeled_end_time'] = modeled_end_time
        self['modeled_timestep'] = modeled_timestep
        self['models'] = models
        self['connections'] = connections
        self['mediations'] = mediations
        self['sequence'] = sequence

    @property
    def modeling_system(self) -> ModelingSystem:
        modeling_system = ModelingSystem(
            start_time=self['modeled_start_time'],
            end_time=self['modeled_end_time'],
            interval=self['modeled_timestep'],
            **{model.model_type.value.lower(): model
               for model in self['models']},
        )
        for connection in self['connections']:
            modeling_system.connect(*connection)
        for mediation in self['mediations']:
            modeling_system.mediate(*mediation)

        modeling_system.sequence = self['sequence']

        return modeling_system


class ModelConfiguration(Configuration):
    def __init__(self, model: Model, fields: {str: type}):
        super().__init__(fields=fields)
        if not isinstance(model, Model):
            model = Model[str(model).lower()]
        self.model = model

    @property
    def name(self) -> str:
        return self.model.value


class ADCIRCConfiguration(ModelConfiguration):
    name = 'ADCIRC'

    def __init__(
        self,
        adcprep_executable_path: PathLike,
        modeled_start_time: datetime,
        modeled_end_time: datetime,
        modeled_timestep: timedelta,
        fort_13_path: PathLike,
        fort_14_path: PathLike,
        write_surface_output: bool = True,
        write_station_output: bool = False,
        use_original_mesh: bool = False,
        stations_file_path: PathLike = None,
        tidal_spinup_duration: timedelta = None,
        tidal_spinup_timestep: timedelta = None,
        gwce_solution_scheme: str = None,
        use_smagorinsky: bool = None,
        use_baroclinicity: bool = None,
        forcings: [Forcing] = None,
        slurm_configuration: SlurmConfiguration = None,
    ):
        if tidal_spinup_timestep is None:
            tidal_spinup_timestep = modeled_timestep

        if forcings is None:
            forcings = []

        super().__init__(
            model=Model.ADCIRC,
            fields={
                'adcprep_executable_path': Path,
                'modeled_start_time': datetime,
                'modeled_end_time': datetime,
                'modeled_timestep': timedelta,
                'fort_13_path': Path,
                'fort_14_path': Path,
                'write_surface_output': bool,
                'write_station_output': bool,
                'use_original_mesh': bool,
                'stations_file_path': Path,
                'tidal_spinup_duration': timedelta,
                'tidal_spinup_timestep': timedelta,
                'gwce_solution_scheme': GWCESolutionScheme,
                'use_smagorinsky': bool,
                'use_baroclinicity': bool,
            },
        )

        self['adcprep_executable_path'] = adcprep_executable_path
        self['modeled_start_time'] = modeled_start_time
        self['modeled_end_time'] = modeled_end_time
        self['modeled_timestep'] = modeled_timestep
        self['fort_13_path'] = fort_13_path
        self['fort_14_path'] = fort_14_path
        self['write_surface_output'] = write_surface_output
        self['write_station_output'] = write_station_output
        self['use_original_mesh'] = use_original_mesh
        self['stations_file_path'] = stations_file_path
        self['tidal_spinup_duration'] = tidal_spinup_duration
        self['tidal_spinup_timestep'] = tidal_spinup_timestep
        self['gwce_solution_scheme'] = gwce_solution_scheme
        self['use_smagorinsky'] = use_smagorinsky
        self['use_baroclinicity'] = use_baroclinicity

        self.forcings = forcings
        self.slurm_configuration = slurm_configuration

    @property
    def mesh(self) -> AdcircMesh:
        LOGGER.info(f'opening mesh "{self["fort_14_path"]}"')
        mesh = AdcircMesh.open(self['fort_14_path'].absolute(), crs=4326)

        if self['fort_13_path'] is not None and self['fort_13_path'].exists():
            mesh.import_nodal_attributes(self['fort_13_path'])
        else:
            LOGGER.warning(f'mesh values (nodal attributes) not found at '
                           f'"{self["fort_13_path"]}"')

        LOGGER.debug(f'adding {len(self.forcings)} forcing(s) to mesh')
        for forcing in self.forcings:
            mesh.add_forcing(forcing)

        if not mesh.has_nodal_attribute('primitive_weighting_in_'
                                        'continuity_equation'):
            LOGGER.debug(f'generating tau0 in mesh')
            mesh.generate_tau0()

        return mesh

    @property
    def driver(self) -> AdcircRun:
        # instantiate AdcircRun object.
        driver = AdcircRun(
            mesh=self.mesh,
            start_date=self['modeled_start_time'],
            end_date=self['modeled_end_time'],
            spinup_time=self['tidal_spinup_duration'],
            server_config=self.slurm_configuration,
        )

        if self['modeled_timestep'] is not None:
            driver.timestep = self['modeled_timestep'] / timedelta(seconds=1)

        if self['gwce_solution_scheme'] is not None:
            driver.gwce_solution_scheme = self['gwce_solution_scheme'].value

        if self['use_smagorinsky'] is not None:
            driver.smagorinsky = self['use_smagorinsky']

        if self['use_baroclinicity'] is not None:
            driver.baroclinicity = self['use_baroclinicity']

        # spinup_start = self['modeled_start_time'] - self['tidal_spinup_duration']
        # spinup_end = self['modeled_start_time']

        if self['write_station_output'] and self['stations_file_path'].exists():
            driver.import_stations(self['stations_file_path'])
            driver.set_elevation_stations_output(self['modeled_timestep'],
                                                 spinup=self['tidal_spinup_timestep'])
            # spinup_start=spinup_start, spinup_end=spinup_end)
            driver.set_velocity_stations_output(self['modeled_timestep'],
                                                spinup=self['tidal_spinup_timestep'])
            # spinup_start=spinup_start, spinup_end=spinup_end)

        if self['write_surface_output']:
            driver.set_elevation_surface_output(self['modeled_timestep'],
                                                spinup=self['tidal_spinup_timestep'])
            # spinup_start=spinup_start, spinup_end=spinup_end)
            driver.set_velocity_surface_output(self['modeled_timestep'],
                                               spinup=self['tidal_spinup_timestep'])
            # spinup_start=spinup_start, spinup_end=spinup_end)

        return driver


class ForcingConfiguration(ModelConfiguration, ABC):
    def __init__(
        self,
        model: Model,
        resource: PathLike,
        fields: {str: type} = None,
    ):
        if fields is None:
            fields = {}

        fields.update({'resource': Path})

        super().__init__(model, fields)

        self['resource'] = resource

    @property
    @abstractmethod
    def forcing(self) -> Forcing:
        raise NotImplementedError

    def name(self) -> str:
        return self.model.value


class TidalForcingConfiguration(ForcingConfiguration):
    name = 'TidalForcing'

    def __init__(
        self,
        resource: PathLike = None,
        tidal_source: TidalSource = TidalSource.TPXO,
        constituents: [str] = None,
    ):
        if constituents is None:
            constituents = 'ALL'

        super().__init__(
            model=Model.Tides,
            resource=resource,
            fields={
                'tidal_source': TidalSource,
                'constituents': [str],
            }
        )

        self['tidal_source'] = tidal_source
        self['constituents'] = constituents

    @property
    def forcing(self) -> Forcing:
        tides = Tides(
            tidal_source=self['tidal_source'],
            resource=self['resource'],
        )

        constituents = [constituent.upper()
                        for constituent in self['constituents']]
        if 'ALL' in constituents:
            tides.use_all()
        elif 'MAJOR' in constituents:
            tides.use_major()
        else:
            for constituent in constituents:
                tides.use_constituent(constituent)

        return tides


class ATMESHForcingConfiguration(ForcingConfiguration):
    name = 'ATMESH'

    def __init__(
        self,
        resource: PathLike,
        nws: int = 17,
        modeled_timestep: timedelta = timedelta(hours=1),
    ):
        super().__init__(
            model=Model.ATMesh,
            resource=resource,
            fields={
                'NWS': int,
                'modeled_timestep': timedelta,
            }
        )

        self['NWS'] = nws
        self['modeled_timestep'] = modeled_timestep

    @property
    def forcing(self) -> Forcing:
        return AtmosphericMeshForcing(
            nws=self['NWS'],
            interval_seconds=self['modeled_timestep'] / timedelta(seconds=1),
        )


class WW3DATAForcingConfiguration(ForcingConfiguration):
    name = 'WW3DATA'

    def __init__(
        self,
        resource: PathLike,
        nrs: int = 5,
        modeled_timestep: timedelta = timedelta(hours=1),
    ):
        super().__init__(
            model=Model.WW3Data,
            resource=resource,
            fields={
                'NRS': int,
                'modeled_timestep': timedelta,
            }
        )

        self['NRS'] = nrs
        self['modeled_timestep'] = modeled_timestep

    @property
    def forcing(self) -> Forcing:
        return WaveWatch3DataForcing(
            nrs=self['nrs'],
            interval_seconds=self['modeled_timestep'],
        )


class CoupledModelDriverConfiguration(Configuration):
    name = 'CoupledModelDriver'

    def __init__(
        self,
        platform: Platform,
        output_directory: PathLike,
        models: [Model],
        runs: {str: (str, Any)},
        verbose: bool = False,
    ):
        super().__init__(fields={
            'platform': Platform,
            'output_directory': Path,
            'models': [Model],
            'runs': {str: (str, Any)},
            'verbose': bool,
        })

        self['platform'] = platform
        self['output_directory'] = output_directory
        self['models'] = models
        self['runs'] = runs
        self['verbose'] = verbose
