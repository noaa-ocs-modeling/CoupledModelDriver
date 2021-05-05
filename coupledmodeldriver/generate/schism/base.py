from datetime import datetime, timedelta
from os import PathLike
from pathlib import Path
from typing import Any

from pyproj import CRS
from pyschism import ModelDomain, ModelDriver, Stations
from pyschism.enums import Stratification
from pyschism.mesh import Fgrid, Hgrid, Vgrid
from pyschism.server import ServerConfig

from coupledmodeldriver.configure import CirculationModelJSON
from coupledmodeldriver.configure.base import AttributeJSON, \
    NEMSCapJSON

SCHISM_ATTRIBUTES = {}


class SCHISMJSON(CirculationModelJSON, NEMSCapJSON, AttributeJSON):
    name = 'SCHISM'
    default_filename = f'configure_schism.json'
    default_processors = 11
    default_attributes = SCHISM_ATTRIBUTES

    field_types = {
        'modeled_timestep': timedelta,
        'rnday': timedelta,
        'tidal_spinup_duration': timedelta,
        'start_date': datetime,
        'ibc': Stratification,
        'drampbc': timedelta,
        'stations_file_path': Stations,
        'stations_frequency': timedelta,
        'stations_crs': CRS,
        'nhot_write': timedelta,
        'server_config': ServerConfig,
        'combine_hotstart': Path,
        'cutoff_depth': float,
        'output_frequency': timedelta,
        'output_new_file_frequency': int,
        'surface_outputs': {str: Any},
    }

    def __init__(
        self,
        mesh_files: [PathLike],
        executable: PathLike,
        modeled_timestep: timedelta,
        rnday: timedelta,
        tidal_spinup_duration: timedelta,
        start_date: datetime,
        ibc: Stratification,
        drampbc: timedelta,
        stations_file_path: PathLike = None,
        stations_frequency: timedelta = None,
        stations_crs: CRS = None,
        nhot_write: timedelta = None,
        server_config: ServerConfig = None,
        combine_hotstart: PathLike = None,
        cutoff_depth: float = None,
        output_frequency: timedelta = None,
        output_new_file_frequency: int = None,
        surface_outputs: {str: Any} = None,
        **kwargs,
    ):
        if surface_outputs is None:
            surface_outputs = {}

        CirculationModelJSON.__init__(
            self,
            mesh_files=mesh_files,
            executable=executable,
            **kwargs,
        )

        self['modeled_timestep'] = modeled_timestep
        self['rnday'] = rnday
        self['tidal_spinup_duration'] = tidal_spinup_duration
        self['start_date'] = start_date
        self['ibc'] = ibc
        self['drampbc'] = drampbc

        self['stations_file_path'] = stations_file_path
        self['stations_frequency'] = stations_frequency
        self['stations_crs'] = stations_crs

        self['nhot_write'] = nhot_write
        self['server_config'] = server_config
        self['combine_hotstart'] = combine_hotstart
        self['cutoff_depth'] = cutoff_depth

        self['output_frequency'] = output_frequency
        self['output_new_file_frequency'] = output_new_file_frequency
        self['surface_outputs'] = surface_outputs

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
        return Stations.from_file(
            file=self['stations_file_path'],
            nspool_sta=self['stations_frequency'],
            crs=self['stations_crs'],
        )

    @property
    def pyschism_domain(self) -> ModelDomain:
        return ModelDomain(
            hgrid=self.hgrid,
            vgrid=self.vgrid,
            fgrid=self.fgrid,
        )

    @property
    def pyschism_driver(self) -> ModelDriver:
        return ModelDriver(
            model_domain=self.pyschism_domain,
            dt=self['modeled_timestep'],
            rnday=self['rnday'],
            ihfskip=self['output_new_file_frequency'],
            dramp=self['tidal_spinup_duration'],
            start_date=self['start_date'],
            ibc=self['ibc'],
            drampbc=self['drampbc'],
            stations=self.pyschism_stations,
            nspool=self['output_frequency'],
            nhot_write=self['nhot_write'],
            server_config=self['server_config'],
            combine_hotstart=self['combine_hotstart'],
            cutoff_depth=self['cutoff_depth'],
            **self['surface_outputs'],
        )
