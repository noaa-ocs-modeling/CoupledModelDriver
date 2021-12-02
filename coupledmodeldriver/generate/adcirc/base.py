from datetime import datetime, timedelta
from os import PathLike
from pathlib import Path
from typing import Any, Dict, List, Union

from adcircpy import AdcircMesh, AdcircRun, Tides
from adcircpy.forcing import BestTrackForcing
from adcircpy.forcing.base import Forcing
from adcircpy.mesh.fort13 import NodalAttributes
from adcircpy.mesh.mesh import ModelForcings
from adcircpy.server import SlurmConfig
from nemspy.model import ADCIRCEntry

from coupledmodeldriver.configure import Model, ModelJSON, SlurmJSON
from coupledmodeldriver.configure.base import AttributeJSON, NEMSCapJSON
from coupledmodeldriver.configure.configure import from_user_input
from coupledmodeldriver.configure.forcings.base import ForcingJSON
from coupledmodeldriver.utilities import LOGGER

ADCIRCPY_ATTRIBUTES = [
    'predictor_corrector',
    'RUNDES',
    '_IHOT',
    'RUNID',
    'NFOVER',
    'WarnElev',
    'iWarnElevDump',
    'WarnElevDumpLimit',
    'ErrorElev',
    'NABOUT',
    'NSCREEN',
    'IDEN',
    'NOLIBF',
    'NOLICA',
    'NOLICAT',
    'NCOR',
    'NTIP',
    'G',
    'TAU0',
    'DTDP',
    'STATIM',
    'REFTIM',
    'DRAMP',
    'DRAMPExtFlux',
    'FluxSettlingTime',
    'DRAMPIntFlux',
    'DRAMPElev',
    'DRAMPTip',
    'DRAMPMete',
    'DRAMPWRad',
    'DUnRampMete',
    'H0',
    'NODEDRYMIN',
    'NODEWETRMP',
    'VELMIN',
    'SLAM0',
    'SFEA0',
    'FFACTOR',
    'CF',
    'HBREAK',
    'FTHETA',
    'FGAMMA',
    'ESLM',
    'NOUTGE',
    'TOUTSGE',
    'TOUTFGE',
    'NSPOOLGE',
    'NOUTGV',
    'TOUTSGV',
    'TOUTFGV',
    'NSPOOLGV',
    'NOUTGM',
    'TOUTSGM',
    'TOUTFGM',
    'NSPOOLGM',
    'NOUTGC',
    'TOUTSGC',
    'TOUTFGC',
    'NSPOOLGC',
    'CORI',
    'ANGINN',
    'THAS',
    'THAF',
    'NHAINC',
    'FMV',
    'NHSTAR',
    'NHSINC',
    'ITITER',
    'ISLDIA',
    'CONVCR',
    'ITMAX',
    'NCPROJ',
    'NCINST',
    'NCSOUR',
    'NCHIST',
    'NCREF',
    'NCCOM',
    'NCHOST',
    'NCCONV',
    'NCCOUT',
    'vertical_mode',
    'lateral_stress_in_gwce',
    'lateral_stress_in_gwce_is_symmetrical',
    'adcvection_in_gwce',
    'lateral_stress_in_momentum',
    'lateral_stress_in_momentum_is_symmetrical',
    'lateral_stress_in_momentum_method',
    'adcvection_in_momentum',
    'area_integration_in_momentum',
    'baroclinicity',
    'gwce_solution_scheme',
    'passive_scalar_transport',
    'stress_based_3D',
    'smagorinsky',
    'smagorinsky_coefficient',
    'horizontal_mixing_coefficient',
    'CFL',
    'ICS',
]
OUTPUT_INTERVAL_DEFAULTS = {
    'surface_output_interval': timedelta(hours=1),
    'stations_output_interval': timedelta(minutes=6),
}


class ADCIRCJSON(ModelJSON, NEMSCapJSON, AttributeJSON):
    """
    ADCIRC configuration in ``configure_adcirc.json``

    stores a number of ADCIRC parameters (``ICS``, ``IM``, etc.) and optionally NEMS parameters

    .. code-block:: python

        configuration = ADCIRCJSON(
            adcirc_executable_path='adcirc',
            adcprep_executable_path='adcprep',
            modeled_start_time=datetime(2012, 10, 22, 6),
            modeled_end_time=datetime(2012, 10, 22, 6) + timedelta(days=14.5),
            modeled_timestep=timedelta(seconds=2),
            fort_13_path=None,
            fort_14_path=INPUT_DIRECTORY / 'meshes' / 'shinnecock' / 'fort.14',
            tidal_spinup_duration=timedelta(days=12.5),
        )

    """

    name = 'ADCIRC'
    default_filename = f'configure_adcirc.json'
    default_processors = 11
    default_attributes = ADCIRCPY_ATTRIBUTES

    field_types = {
        'adcirc_executable_path': Path,
        'adcprep_executable_path': Path,
        'aswip_executable_path': Path,
        'modeled_start_time': datetime,
        'modeled_end_time': datetime,
        'modeled_timestep': timedelta,
        'fort_13_path': Path,
        'fort_14_path': Path,
        'tidal_spinup_duration': timedelta,
        'tidal_spinup_timestep': timedelta,
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
        forcings: List[Forcing] = None,
        aswip_executable_path: PathLike = None,
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
        nems_parameters: Dict[str, str] = None,
        attributes: Dict[str, Any] = None,
        **kwargs,
    ):
        """
        :param adcirc_executable_path: file path to ``adcirc`` or ``NEMS.x``
        :param adcprep_executable_path: file path to ``adcprep``
        :param aswip_executable_path: file path to ``aswip``
        :param modeled_start_time: start time in model run
        :param modeled_end_time: edn time in model run
        :param modeled_timestep: time interval between model steps
        :param fort_13_path: file path to ``fort.13``
        :param fort_14_path: file path to ``fort.14``
        :param tidal_spinup_duration: tidal spinup duration for ADCIRC coldstart
        :param tidal_spinup_timestep: tidal spinup modeled time interval for ADCIRC coldstart
        :param forcings: list of Forcing objects to apply to the mesh
        :param source_filename: path to modulefile to ``source``
        :param slurm_configuration: Slurm configuration object
        :param use_original_mesh: whether to symlink / copy original mesh instead of rewriting with ``adcircpy``
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
        :param attributes: attributes to set in ``adcircpy.AdcircRun`` object
        """

        self.__mesh = None
        self.__base_mesh = None

        if tidal_spinup_timestep is None:
            tidal_spinup_timestep = modeled_timestep

        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(ADCIRCJSON.field_types)

        ModelJSON.__init__(self, model=Model.ADCIRC, **kwargs)
        NEMSCapJSON.__init__(
            self, processors=processors, nems_parameters=nems_parameters, **kwargs
        )
        AttributeJSON.__init__(self, attributes=attributes, **kwargs)

        self['adcirc_executable_path'] = adcirc_executable_path
        self['adcprep_executable_path'] = adcprep_executable_path
        self['aswip_executable_path'] = aswip_executable_path
        self['modeled_start_time'] = modeled_start_time
        self['modeled_end_time'] = modeled_end_time
        self['modeled_timestep'] = modeled_timestep
        self['fort_13_path'] = fort_13_path
        self['fort_14_path'] = fort_14_path
        self['tidal_spinup_duration'] = tidal_spinup_duration
        self['tidal_spinup_timestep'] = tidal_spinup_timestep
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

        self.__forcings = []
        self.forcings = forcings
        self.slurm_configuration = slurm_configuration

        for output_interval_entry, default_interval in OUTPUT_INTERVAL_DEFAULTS.items():
            if self[output_interval_entry] is None:
                LOGGER.debug(f'setting `{output_interval_entry}` to {default_interval}')
                self[output_interval_entry] = default_interval

    @property
    def forcings(self) -> List[ForcingJSON]:
        return list(self.__forcings)

    @forcings.setter
    def forcings(self, forcings: List[ForcingJSON]):
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
    def slurm_configuration(self) -> List[SlurmJSON]:
        return self.__slurm_configuration

    @slurm_configuration.setter
    def slurm_configuration(self, slurm_configuration: SlurmJSON):
        if isinstance(slurm_configuration, SlurmConfig):
            SlurmJSON.from_adcircpy(slurm_configuration)
        self.__slurm_configuration = slurm_configuration

    @property
    def adcircpy_forcings(self) -> List[Forcing]:
        return [forcing.adcircpy_forcing for forcing in self.forcings]

    @property
    def adcircpy_mesh(self) -> AdcircMesh:
        if self.__mesh is None:
            mesh = self.base_mesh.copy()

            # reconstruct mesh from base
            mesh.forcings = ModelForcings(mesh)
            mesh.nodal_attributes = NodalAttributes(mesh)

            if self['fort_13_path'] is not None:
                if self['fort_13_path'].exists():
                    LOGGER.info(f'reading attributes from "{self["fort_13_path"]}"')
                    mesh.import_nodal_attributes(self['fort_13_path'])
                    for attribute_name in mesh.get_nodal_attribute_names():
                        mesh.set_nodal_attribute_state(
                            attribute_name, coldstart=True, hotstart=True
                        )
                else:
                    LOGGER.warning(
                        f'mesh values (nodal attributes) not found at "{self["fort_13_path"]}"'
                    )

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
                # elif isinstance(adcircpy_forcing, BestTrackForcing):
                #     adcircpy_forcing.clip_to_bbox(mesh.get_bbox(output_type='bbox'), mesh.crs)

                mesh.add_forcing(adcircpy_forcing)

            if not mesh.has_nodal_attribute('primitive_weighting_in_continuity_equation'):
                LOGGER.debug(f'generating tau0 in mesh')
                mesh.generate_tau0()

            self.__mesh = mesh

        return self.__mesh

    @adcircpy_mesh.setter
    def adcircpy_mesh(self, adcircpy_mesh: Union[AdcircMesh, PathLike]):
        self.__mesh = adcircpy_mesh

    @property
    def base_mesh(self) -> AdcircMesh:
        if self.__base_mesh is None:
            if self.__mesh is not None:
                self.__base_mesh = self.__mesh
            else:
                self.__base_mesh = self['fort_14_path']
        if not isinstance(self.__base_mesh, AdcircMesh):
            LOGGER.info(f'opening mesh "{self.__base_mesh}"')
            self.__base_mesh = AdcircMesh.open(self.__base_mesh, crs=4326)
        # deconstruct mesh into a base mesh that can be pickled
        self.__base_mesh.forcings = None
        self.__base_mesh.nodal_attributes = None
        return self.__base_mesh

    @base_mesh.setter
    def base_mesh(self, base_mesh: Union[AdcircMesh, PathLike]):
        self.__base_mesh = base_mesh

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

        for name, value in self['attributes'].items():
            if value is not None:
                try:
                    setattr(driver, name, value)
                except:
                    LOGGER.warning(
                        f'could not set `{driver.__class__.__name__}` attribute `{name}` to `{value}`'
                    )

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

    def __setitem__(self, key: str, value: Any):
        super().__setitem__(key, value)
        self.__mesh = None

    def __copy__(self) -> 'ADCIRCJSON':
        instance = super().__copy__()
        instance.base_mesh = self.base_mesh
        instance.forcings = self.forcings
        return instance
