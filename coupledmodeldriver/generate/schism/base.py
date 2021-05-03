from datetime import datetime, timedelta
from os import PathLike
from typing import Any

from pyschism import ModelDomain, ModelDriver

from coupledmodeldriver.configure import CirculationModelJSON
from coupledmodeldriver.configure.base import AttributeJSON, NEMSCapJSON, SlurmJSON

SCHISM_ATTRIBUTES = {}


class SCHISMJSON(CirculationModelJSON, NEMSCapJSON, AttributeJSON):
    name = 'SCHISM'
    default_filename = f'configure_schism.json'
    default_processors = 11
    default_attributes = SCHISM_ATTRIBUTES

    def __init__(
        self,
        mesh_files: [PathLike],
        executable: PathLike,
        adcprep_executable: PathLike,
        modeled_start_time: datetime,
        modeled_end_time: datetime,
        modeled_timestep: timedelta,
        tidal_spinup_duration: timedelta = None,
        tidal_spinup_timestep: timedelta = None,
        forcings: [Forcing] = None,
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
        attributes: {str: Any} = None,
        **kwargs,
    ):
        super().__init__(executable, **kwargs)

    @property
    def pyschism_domain(self) -> 'ModelDomain':
        return ModelDomain()

    @property
    def pyschism_driver(self) -> 'ModelDriver':
        return ModelDriver()
