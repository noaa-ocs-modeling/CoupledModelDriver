from abc import ABC
from datetime import datetime, timedelta
from enum import Enum
from os import PathLike
from pathlib import Path

from adcircpy import AdcircMesh, AdcircRun, Tides
from adcircpy.forcing.base import Forcing
from adcircpy.forcing.winds import BestTrackForcing
from adcircpy.server import SlurmConfig
from nemspy.model import ADCIRCEntry

from .base import ConfigurationJSON, NEMSCapJSON, SlurmJSON
from .configure import from_user_input
from .forcings.base import ForcingJSON
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


OUTPUT_INTERVAL_DEFAULTS = {
    'surface_output_interval': timedelta(hours=1),
    'stations_output_interval': timedelta(minutes=6),
}


class ADCIRCJSON(ModelJSON, NEMSCapJSON):
    name = 'ADCIRC'
    default_filename = f'configure_adcirc.json'
    default_processors = 11

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
        'output_surface': bool,
        'surface_output_interval': timedelta,
        'output_stations': bool,
        'stations_file_path': Path,
        'stations_output_interval': timedelta,
        'output_spinup': bool,
        'output_elevations': bool,
        'output_velocities': bool,
        'output_concentrations': bool,
        'output_meteorological_factors': bool,
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
        output_surface: bool = True,
        surface_output_interval: timedelta = None,
        output_stations: bool = False,
        stations_output_interval=None,
        stations_file_path: PathLike = None,
        output_spinup: bool = True,
        output_elevations: bool = True,
        output_velocities: bool = True,
        output_concentrations: bool = False,
        output_meteorological_factors: bool = False,
        processors: int = 11,
        nems_parameters: {str: str} = None,
        **kwargs,
    ):
        """
        Instantiate a new ADCIRCJSON configuration.

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
        :param output_surface: write surface (entire mesh) to NetCDF
        :param surface_output_interval: frequency at which output is written to file
        :param output_stations: write stations to NetCDF (only applicable if stations file exists)
        :param stations_output_interval: frequency at which stations output is written to file
        :param stations_file_path: file path to stations file
        :param output_spinup: write spinup to NetCDF
        :param output_elevations: write elevations to NetCDF
        :param output_velocities: write velocities to NetCDF
        :param output_concentrations: write concentrations to NetCDF
        :param output_meteorological_factors: write meteorological factors to NetCDF
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

        self['output_surface'] = output_surface
        self['surface_output_interval'] = surface_output_interval
        self['output_stations'] = output_stations
        self['stations_output_interval'] = stations_output_interval
        self['stations_file_path'] = stations_file_path
        self['output_spinup'] = output_spinup
        self['output_elevations'] = output_elevations
        self['output_velocities'] = output_velocities
        self['output_concentrations'] = output_concentrations
        self['output_meteorological_factors'] = output_meteorological_factors

        self.forcings = forcings
        self.slurm_configuration = slurm_configuration

        for output_interval_entry, default_interval in OUTPUT_INTERVAL_DEFAULTS.items():
            if self[output_interval_entry] is None:
                LOGGER.debug(f'setting `{output_interval_entry}` to {default_interval}')
                self[output_interval_entry] = default_interval

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
        if not isinstance(forcing, ForcingJSON):
            forcing = from_user_input(forcing)
        adcircpy_forcing = forcing.adcircpy_forcing

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
            if isinstance(adcircpy_forcing, (Tides, BestTrackForcing)):
                adcircpy_forcing.start_date = self['modeled_start_time']
                adcircpy_forcing.end_date = self['modeled_end_time']

            if (
                isinstance(adcircpy_forcing, Tides)
                and self['tidal_spinup_duration'] is not None
            ):
                adcircpy_forcing.spinup_time = self['tidal_spinup_duration']
                adcircpy_forcing.start_date -= self['tidal_spinup_duration']
            elif isinstance(adcircpy_forcing, BestTrackForcing):
                adcircpy_forcing.clip_to_bbox(mesh.get_bbox(output_type='bbox'), mesh.crs)

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

        if self['stations_file_path'] is not None:
            LOGGER.info(f'importing stations from "{self["stations_file_path"]}"')
            driver.import_stations(self['stations_file_path'])

        if self['modeled_timestep'] is not None:
            driver.timestep = self['modeled_timestep'] / timedelta(seconds=1)

        if self['gwce_solution_scheme'] is not None:
            driver.gwce_solution_scheme = self['gwce_solution_scheme'].value

        if self['use_smagorinsky'] is not None:
            driver.smagorinsky = self['use_smagorinsky']

        if self['tidal_spinup_duration'] is not None and self['output_spinup']:
            spinup_start = self['modeled_start_time'] - self['tidal_spinup_duration']
            spinup_end = self['modeled_start_time']
            spinup_output_interval = self['surface_output_interval']
        else:
            spinup_start = None
            spinup_end = None
            spinup_output_interval = None

        if self['output_elevations']:
            if self['output_surface']:
                driver.set_elevation_surface_output(
                    sampling_rate=self['surface_output_interval'],
                    start=self['modeled_start_time'],
                    end=self['modeled_end_time'],
                    spinup=spinup_output_interval,
                    spinup_start=spinup_start,
                    spinup_end=spinup_end,
                )
            if self['output_stations']:
                driver.set_elevation_stations_output(
                    sampling_rate=self['stations_output_interval'],
                    start=self['modeled_start_time'],
                    end=self['modeled_end_time'],
                    spinup=spinup_output_interval,
                    spinup_start=spinup_start,
                    spinup_end=spinup_end,
                )

        if self['output_velocities']:
            if self['output_surface']:
                driver.set_velocity_surface_output(
                    sampling_rate=self['surface_output_interval'],
                    start=self['modeled_start_time'],
                    end=self['modeled_end_time'],
                    spinup=spinup_output_interval,
                    spinup_start=spinup_start,
                    spinup_end=spinup_end,
                )
            if self['output_stations']:
                driver.set_velocity_stations_output(
                    sampling_rate=self['stations_output_interval'],
                    start=self['modeled_start_time'],
                    end=self['modeled_end_time'],
                    spinup=spinup_output_interval,
                    spinup_start=spinup_start,
                    spinup_end=spinup_end,
                )

        if self['output_concentrations']:
            if self['output_surface']:
                driver.set_concentration_surface_output(
                    sampling_rate=self['surface_output_interval'],
                    start=self['modeled_start_time'],
                    end=self['modeled_end_time'],
                    spinup=spinup_output_interval,
                    spinup_start=spinup_start,
                    spinup_end=spinup_end,
                )
            if self['output_stations']:
                driver.set_concentration_stations_output(
                    sampling_rate=self['stations_output_interval'],
                    start=self['modeled_start_time'],
                    end=self['modeled_end_time'],
                    spinup=spinup_output_interval,
                    spinup_start=spinup_start,
                    spinup_end=spinup_end,
                )

        if self['output_meteorological_factors']:
            if self['output_surface']:
                driver.set_meteorological_surface_output(
                    sampling_rate=self['surface_output_interval'],
                    start=self['modeled_start_time'],
                    end=self['modeled_end_time'],
                    spinup=spinup_output_interval,
                    spinup_start=spinup_start,
                    spinup_end=spinup_end,
                )
            if self['output_stations']:
                driver.set_meteorological_stations_output(
                    sampling_rate=self['stations_output_interval'],
                    start=self['modeled_start_time'],
                    end=self['modeled_end_time'],
                    spinup=spinup_output_interval,
                    spinup_start=spinup_start,
                    spinup_end=spinup_end,
                )

        return driver

    @property
    def nemspy_entry(self) -> ADCIRCEntry:
        return ADCIRCEntry(processors=self['processors'], **self['nems_parameters'])
