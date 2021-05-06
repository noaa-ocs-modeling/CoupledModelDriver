from datetime import datetime, timedelta
from os import PathLike
from pathlib import Path
from typing import Union

from pyproj import CRS
from pyschism import ModelDomain, ModelDriver, Stations
from pyschism.enums import IofHydroVariables, Stratification
from pyschism.forcing import Hydrology, Tides
from pyschism.forcing.atmosphere import NWS2
from pyschism.forcing.atmosphere.nws.nws2.sflux import SfluxDataset
from pyschism.mesh import Fgrid, Hgrid, Vgrid

from coupledmodeldriver.configure import CirculationModelJSON
from coupledmodeldriver.configure.base import NEMSCapJSON, SlurmJSON
from coupledmodeldriver.configure.configure import from_user_input
from coupledmodeldriver.configure.forcings.base import ForcingJSON, PYSCHISM_FORCING_CLASSES


class SCHISMJSON(CirculationModelJSON, NEMSCapJSON):
    name = 'SCHISM'
    default_filename = f'configure_schism.json'
    default_processors = 11

    field_types = {
        'modeled_start_time': datetime,
        'modeled_duration': timedelta,
        'modeled_timestep': timedelta,
        'tidal_spinup_duration': timedelta,
        'tidal_bc_spinup_duration': timedelta,
        'tidal_bc_cutoff_depth': float,
        'stratification': Stratification,
        'hotstart_output_interval': timedelta,
        'hotstart_combination_executable': Path,
        'output_surface': bool,
        'surface_output_interval': timedelta,
        'surface_output_new_file_skips': int,
        'surface_output_variables': {str: bool},
        'output_stations': bool,
        'stations_output_interval': timedelta,
        'stations_file_path': Stations,
        'stations_crs': CRS,
    }

    def __init__(
        self,
        mesh_files: [PathLike],
        executable: PathLike,
        modeled_start_time: datetime,
        modeled_duration: timedelta,
        modeled_timestep: timedelta,
        tidal_spinup_duration: timedelta,
        tidal_bc_spinup_duration: timedelta,
        tidal_bc_cutoff_depth: float = None,
        forcings: [ForcingJSON] = None,
        stratification: Stratification = None,
        hotstart_output_interval: timedelta = None,
        slurm_configuration: SlurmJSON = None,
        hotstart_combination_executable: PathLike = None,
        output_surface: bool = None,
        surface_output_interval: timedelta = None,
        surface_output_new_file_skips: int = None,
        surface_output_variables: {IofHydroVariables: bool} = None,
        output_stations: bool = None,
        stations_output_interval: timedelta = None,
        stations_file_path: PathLike = None,
        stations_crs: CRS = None,
        **kwargs,
    ):
        """
        Instantiate a new SCHISMJSON configuration.

        :param mesh_files: file paths to grid files
        :param executable: file path to `schism` or `NEMS.x`
        :param modeled_start_time: start time in model run
        :param modeled_duration: duration of model run
        :param modeled_timestep: time interval between model steps
        :param tidal_spinup_duration: tidal spinup duration for SCHISM coldstart
        :param tidal_bc_spinup_duration: BC tidal spinup duration for SCHISM coldstart
        :param tidal_bc_cutoff_depth: cutoff depth for `bctides.in`
        :param forcings: list of forcing configurations to apply to the mesh
        :param stratification: IBC parameter; one of [`BAROCLINIC`, `BAROTROPIC`]
        :param hotstart_output_interval: hotstart output interval
        :param server_config: `ServerConfig` object
        :param hotstart_combination_executable: file path to hotstart combination executable
        :param output_surface: write surface (entire mesh)
        :param surface_output_interval: time interval at which to output surface
        :param surface_output_new_file_skips: number of intervals to skip between output
        :param surface_output_variables: variables to output to surface
        :param output_stations: write stations
        :param stations_output_interval:
        :param stations_file_path: file path to stations file
        :param stations_crs: coordinate reference system of stations
        """

        if stratification is None:
            stratification = Stratification.BAROTROPIC

        if output_surface is None:
            output_surface = False

        if output_stations is None:
            output_stations = False

        CirculationModelJSON.__init__(
            self, mesh_files=mesh_files, executable=executable, **kwargs,
        )

        self['modeled_start_time'] = modeled_start_time
        self['modeled_duration'] = modeled_duration
        self['modeled_timestep'] = modeled_timestep

        self['tidal_spinup_duration'] = tidal_spinup_duration
        self['tidal_spinup_duration'] = tidal_bc_spinup_duration

        self['stratification'] = stratification

        self['hotstart_output_interval'] = hotstart_output_interval
        self['hotstart_combination_executable'] = hotstart_combination_executable
        self['cutoff_depth'] = tidal_bc_cutoff_depth

        self['output_surface'] = output_surface
        self['surface_output_interval'] = surface_output_interval
        self['surface_output_new_file_skips'] = surface_output_new_file_skips
        self['surface_output_variables'] = surface_output_variables

        self['output_stations'] = output_stations
        self['stations_output_interval'] = stations_output_interval
        self['stations_file_path'] = stations_file_path
        self['stations_crs'] = stations_crs

        self.slurm_configuration = slurm_configuration

        self.__forcings = []
        self.forcings = forcings

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
        pyschism_forcing = forcing.pyschism_forcing

        existing_forcings = [
            existing_forcing.__class__.__name__ for existing_forcing in self.pyschism_forcings
        ]
        if pyschism_forcing.__class__.__name__ in existing_forcings:
            existing_index = existing_forcings.index(pyschism_forcing.__class__.__name__)
        else:
            existing_index = -1
        if existing_index > -1:
            self.__forcings[existing_index] = forcing
        else:
            self.__forcings.append(forcing)

    @property
    def hgrid_path(self) -> Path:
        """
        :return: file path to horizontal grid
        """
        for mesh_file in self['mesh_files']:
            if 'hgrid' in str(mesh_file).lower():
                return mesh_file
        else:
            return None

    @property
    def vgrid_path(self) -> Path:
        """
        :return: file path to vertical grid
        """
        for mesh_file in self['mesh_files']:
            if 'vgrid' in str(mesh_file).lower():
                return mesh_file
        else:
            return None

    @property
    def fgrid_path(self) -> Path:
        """
        :return: file path to friction grid
        """
        for mesh_file in self['mesh_files']:
            if 'fgrid' in str(mesh_file).lower():
                return mesh_file
        else:
            return None

    @property
    def hgrid(self) -> Hgrid:
        """
        :return: horizontal grid
        """
        return Hgrid.open(self.hgrid_path)

    @property
    def vgrid(self) -> Vgrid:
        """
        :return: vertical grid
        """
        return Vgrid.open(self.vgrid_path)

    @property
    def fgrid(self) -> Fgrid:
        """
        :return: friction grid
        """
        return Fgrid.open(self.fgrid_path)

    @property
    def pyschism_stations(self) -> Stations:
        if self['output_stations']:
            stations_output_interval = self['stations_output_interval']
        else:
            stations_output_interval = None

        if self['stations_file_path'] is not None:
            stations = Stations.from_file(
                file=self['stations_file_path'],
                nspool_sta=stations_output_interval,
                crs=self['stations_crs'],
            )
        else:
            # TODO implement the extra options here
            stations = Stations(
                nspool_sta=stations_output_interval,
                crs=self['stations_crs'],
                elev=True,
                air_pressure=True,
                windx=True,
                windy=True,
                T=True,
                S=True,
                u=True,
                v=True,
                w=True,
            )
        return stations

    @property
    def pyschism_forcings(self) -> Union[PYSCHISM_FORCING_CLASSES]:
        return [forcing.pyschism_forcing for forcing in self.forcings]

    @property
    def pyschism_domain(self) -> ModelDomain:
        domain = ModelDomain(hgrid=self.hgrid, vgrid=self.vgrid, fgrid=self.fgrid)
        sflux_forcings = []
        for pyschism_forcing in self.pyschism_forcings:
            if isinstance(pyschism_forcing, Hydrology):
                domain.add_hydrology(pyschism_forcing)
            elif isinstance(pyschism_forcing, SfluxDataset):
                sflux_forcings.append(pyschism_forcing)
            elif isinstance(pyschism_forcing, Tides):
                domain.add_boundary_condition(pyschism_forcing)
            # TODO add more atmospheric forcings
        if len(sflux_forcings) > 0:
            if len(sflux_forcings) > 2:
                raise NotImplementedError('more than 2 sflux forcings not implemented')
            domain.set_atmospheric_forcing(
                NWS2(sflux_1=sflux_forcings[0], sflux_2=sflux_forcings[1])
            )
        return domain

    @property
    def pyschism_driver(self) -> ModelDriver:
        if self.slurm_configuration is not None:
            server_configuration = self.slurm_configuration.to_pyschism
        else:
            server_configuration = None

        surface_output_variables = {
            key.value: value for key, value in self['surface_output_variables'].items()
        }

        driver = ModelDriver(
            model_domain=self.pyschism_domain,
            dt=self['modeled_timestep'],
            rnday=self['modeled_duration'],
            ihfskip=self['surface_output_new_file_interval'],
            dramp=self['tidal_spinup_duration'],
            start_date=self['start_date'],
            ibc=self['stratification'],
            drampbc=self['tidal_bc_spinup_duration'],
            stations=self.pyschism_stations,
            nspool=self['surface_output_interval'],
            nhot_write=self['hotstart_output_interval'],
            server_config=server_configuration,
            combine_hotstart=self['hotstart_combination_executable'],
            cutoff_depth=self['tidal_bc_cutoff_depth'],
            **surface_output_variables,
        )

        return driver
