from abc import ABC
from datetime import datetime, timedelta
from enum import Enum
from os import PathLike
from pathlib import Path

from adcircpy import AdcircMesh, AdcircRun, Tides
from adcircpy.forcing.base import Forcing
from adcircpy.server import SlurmConfig
from nemspy.model import ADCIRCEntry

from .base import ConfigurationJSON, NEMSCapJSON, SlurmJSON
from .forcings.base import ADCIRCPY_FORCING_CLASSES, ForcingJSON
from ..utilities import LOGGER


class Model(Enum):
    ADCIRC = 'ADCIRC'
    TidalForcing = 'Tides'
    ATMESH = 'ATMESH'
    WW3DATA = 'WW3DATA'


class ModelJSON(ConfigurationJSON, ABC):
    def __init__(self, model: Model, **kwargs):
        if not isinstance(model, Model):
            model = Model[str(model).lower()]

        ConfigurationJSON.__init__(self, **kwargs)

        self.model = model


class GWCESolutionScheme(Enum):
    explicit = 'explicit'
    semi_implicit = 'semi-implicit'
    semi_implicit_legacy = 'semi-implicit-legacy'


class ADCIRCJSON(ModelJSON, NEMSCapJSON):
    name = 'ADCIRC'
    default_filename = f'configure_adcirc.json'
    field_types = {
        'adcirc_executable_path': Path,
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
        adcirc_executable_path: PathLike,
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
        **kwargs,
    ):
        """

        :param adcirc_executable_path: file path to `adcirc` or `NEMS.x`
        :param adcprep_executable_path: file path to `adcprep`
        :param modeled_start_time: start time in model run
        :param modeled_end_time: edn time in model run
        :param modeled_timestep: time interval between model steps
        :param fort_13_path: file path to `fort.13`
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

        self.__forcings = []

        if tidal_spinup_timestep is None:
            tidal_spinup_timestep = modeled_timestep
        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(ADCIRCJSON.field_types)

        ModelJSON.__init__(self, model=Model.ADCIRC, **kwargs)
        NEMSCapJSON.__init__(
            self, processors=processors, nems_parameters=nems_parameters, **kwargs
        )

        self['adcirc_executable_path'] = adcirc_executable_path
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
    def forcings(self) -> [ForcingJSON]:
        return list(self.__forcings)

    @forcings.setter
    def forcings(self, forcings: [ForcingJSON]):
        if forcings is None:
            forcings = []
        for forcing in forcings:
            self.add_forcing(forcing)

    def add_forcing(self, forcing: ForcingJSON):
        if isinstance(forcing, str):
            try:
                forcing = ForcingJSON.from_file(Path(forcing))
            except:
                forcing = ForcingJSON.from_string(forcing)
        if isinstance(forcing, ADCIRCPY_FORCING_CLASSES):
            adcircpy_forcing = forcing
            forcing = ForcingJSON.from_adcircpy(forcing)
        elif isinstance(forcing, ForcingJSON):
            adcircpy_forcing = forcing.adcircpy_forcing
        else:
            raise NotImplementedError(f'unrecognized forcing type {type(forcing)}')

        existing_forcings = [
            existing_forcing.__class__.__name__ for existing_forcing in self.adcircpy_forcings
        ]
        if adcircpy_forcing.__class__.__name__ in existing_forcings:
            existing_index = existing_forcings.index(adcircpy_forcing.__class__.__name__)
        else:
            existing_index = -1
        if existing_index > -1:
            self.__forcings[existing_index] = forcing
        else:
            self.__forcings.append(forcing)

    @property
    def slurm_configuration(self) -> [SlurmJSON]:
        return self.__slurm_configuration

    @slurm_configuration.setter
    def slurm_configuration(self, slurm_configuration: SlurmJSON):
        if isinstance(slurm_configuration, SlurmConfig):
            SlurmJSON.from_adcircpy(slurm_configuration)
        self.__slurm_configuration = slurm_configuration

    @property
    def adcircpy_forcings(self) -> [Forcing]:
        return [forcing.adcircpy_forcing for forcing in self.forcings]

    @property
    def adcircpy_mesh(self) -> AdcircMesh:
        LOGGER.info(f'opening mesh "{self["fort_14_path"]}"')
        mesh = AdcircMesh.open(self['fort_14_path'], crs=4326)

        LOGGER.debug(f'adding {len(self.forcings)} forcing(s) to mesh')
        for adcircpy_forcing in self.adcircpy_forcings:
            if (
                isinstance(adcircpy_forcing, Tides)
                and self['tidal_spinup_duration'] is not None
            ):
                adcircpy_forcing.spinup_time = self['tidal_spinup_duration']
                adcircpy_forcing.start_date = self['modeled_start_time']
                if self['tidal_spinup_duration'] is not None:
                    adcircpy_forcing.start_date -= self['tidal_spinup_duration']
                adcircpy_forcing.end_date = self['modeled_end_time']
            mesh.add_forcing(adcircpy_forcing)

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
                    f'mesh values (nodal attributes) not found at "{self["fort_13_path"]}"'
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

        if self['tidal_spinup_duration'] is not None:
            spinup_start = self['modeled_start_time'] - self['tidal_spinup_duration']
        else:
            spinup_start = None

        if self['write_station_output'] and self['stations_file_path'].exists():
            driver.import_stations(self['stations_file_path'])
            driver.set_elevation_stations_output(
                self['modeled_timestep'],
                spinup=self['tidal_spinup_timestep'],
                spinup_start=spinup_start,
            )
            driver.set_velocity_stations_output(
                self['modeled_timestep'],
                spinup=self['tidal_spinup_timestep'],
                spinup_start=spinup_start,
            )

        if self['write_surface_output']:
            driver.set_elevation_surface_output(
                self['modeled_timestep'],
                spinup=self['tidal_spinup_timestep'],
                spinup_start=spinup_start,
            )
            driver.set_velocity_surface_output(
                self['modeled_timestep'],
                spinup=self['tidal_spinup_timestep'],
                spinup_start=spinup_start,
            )

        return driver

    @property
    def nemspy_entry(self) -> ADCIRCEntry:
        return ADCIRCEntry(processors=self['processors'], **self['nems_parameters'])
