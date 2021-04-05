from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from os import PathLike
from pathlib import Path

from adcircpy import AdcircMesh, AdcircRun, Tides
from adcircpy.forcing.base import Forcing
from adcircpy.forcing.tides.tides import TidalSource
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing
from adcircpy.server import SlurmConfig
from nemspy.model import ADCIRCEntry, AtmosphericMeshEntry, WaveMeshEntry

from ..configurations import ConfigurationJSON, GWCESolutionScheme, \
    Model, ModelJSON, SlurmJSON
from ..nems.configurations import NEMSCapJSON
from ..utilities import LOGGER


class ADCIRCJSON(ModelJSON, NEMSCapJSON):
    name = 'adcirc'
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
    ):
        """

        :param adcirc_executable_path: file path to `adcirc` or `NEMS.x`
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
            adcircpy_forcing = forcing.adcircpy_forcing
            if isinstance(adcircpy_forcing, Tides) and self['tidal_spinup_duration'] is not None:
                adcircpy_forcing.spinup_time = self['tidal_spinup_duration']
                adcircpy_forcing.start_date = self['modeled_start_time']
                adcircpy_forcing.end_date = self['modeled_end_time']
                if self['tidal_spinup_duration'] is not None:
                    adcircpy_forcing.start_date -= self['tidal_spinup_duration']
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
        else:
            if 'Major' in constituents:
                tides.use_major()
                constituents.remove('Major')
            for constituent in constituents:
                if constituent not in tides.active_constituents:
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
