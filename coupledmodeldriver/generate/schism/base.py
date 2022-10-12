from datetime import datetime, timedelta
from os import PathLike
from pathlib import Path
from typing import Any, List, Union, Dict

from nemspy.model import SCHISMEntry
from pyschism.mesh import Hgrid
from pyschism.server import SlurmConfig
from pyschism.driver import ModelConfig, ModelDriver
from pyschism.forcing.bctides.iettype import TidalElevation
from pyschism.forcing.bctides.ifltype import TidalVelocity
from pyschism.forcing.bctides.tides import Tides
from pyschism.forcing.base import ModelForcing
from pyschism.stations import Stations
from pyschism.forcing.nws.base import NWS
from pyschism.forcing import NWM

from coupledmodeldriver.configure import Model, ModelJSON, SlurmJSON
from coupledmodeldriver.configure.base import AttributeJSON, NEMSCapJSON
from coupledmodeldriver.configure.configure import from_user_input
from coupledmodeldriver.configure.forcings.base import ForcingJSON
from coupledmodeldriver.utilities import LOGGER

PYSCHISM_ATTRIBUTES = [
    'ipre',
    'ibc',
    'ibtp',
    'rnday',
    'dt',
    'nspool',
    'ihfskip',
    'ioffline_partition',
    'ipre2',
    'itransport_only',
    'iloadtide',
    'loadtide_coef',
    'start_year',
    'start_month',
    'start_day',
    'start_hour',
    'utc_start',
    'ics',
    'ihot',
    'ieos_type',
    'ieos_pres',
    'eos_a',
    'eos_b',
    'dramp',
    'drampbc',
    'iupwind_mom',
    'indvel',
    'ihorcon',
    'hvis_coef0',
    'ishapiro',
    'niter_shap',
    'shapiro0',
    'thetai',
    'icou_elfe_wwm',
    'nstep_wwm',
    'iwbl',
    'hmin_radstress',
    'drampwafo',
    'turbinj',
    'turbinjds',
    'alphaw',
    'fwvor_advxy_stokes',
    'fwvor_advz_stokes',
    'fwvor_gradpress',
    'fwvor_breaking',
    'fwvor_streaming',
    'cur_wwm',
    'wafo_obcramp',
    'imm',
    'ibdef',
    'slam0',
    'sfea0',
    'iunder_deep',
    'h1_bcc',
    'h2_bcc',
    'hw_depth',
    'hw_ratio',
    'ihydraulics',
    'if_source',
    'dramp_ss',
    'meth_sink',
    'ihdif',
    'nchi',
    'dzb_min',
    'hmin_man',
    'ncor',
    'rlatitude',
    'coricoef',
    'ic_elev',
    'nramp_elev',
    'inv_atm_bnd',
    'prmsl_ref',
    'gen_wsett',
    'ibcc_mean',
    'rmaxvel',
    'velmin_btrack',
    'ihhat',
    'inunfl',
    'h0',
    'shorewafo',
    'moitn0',
    'mxitn0',
    'rtol0',
    'nadv',
    'dtb_max',
    'dtb_min',
    'inter_mom',
    'kr_co',
    'itr_met',
    'h_tvd',
    'eps1_tvd_imp',
    'eps2_tvd_imp',
    'ielm_transport',
    'max_subcyc',
    'ip_weno',
    'nquad',
    'ntd_weno',
    'epsilon1',
    'epsilon2',
    'i_prtnftl_weno',
    'epsilon3',
    'ielad_weno',
    'small_elad',
    'nws',
    'wtiminc',
    'drampwind',
    'iwindoff',
    'iwind_form',
    'impose_net_flux',
    'ihconsv',
    'isconsv',
    'i_hmin_airsea_ex',
    'hmin_airsea_ex',
    'i_hmin_salt_ex',
    'hmin_salt_ex',
    'itur',
    'dfv0',
    'dfh0',
    'mid',
    'stab',
    'xlsc0',
    'inu_elev',
    'inu_uv',
    'vnh1',
    'vnf1',
    'vnh2',
    'vnf2',
    'step_nu_tr',
    'h_bcc1',
    's1_mxnbt',
    's2_mxnbt',
    'iharind',
    'iflux',
    'izonal5',
    'ibtrack_test',
    'irouse_test',
    'flag_fib',
    'slr_rate',
    'isav',
    'nstep_ice',
    'level_age',
    'rearth_pole',
    'rearth_eq',
    'shw',
    'rho0',
    'h_massconsv',
    'rinflation_icm',
    'nhot',
    'nhot_write',
    'iout_sta',
    'nspool_sta',
]
OUTPUT_INTERVAL_DEFAULTS = {
    'surface_output_interval': timedelta(hours=1),
    'stations_output_interval': timedelta(minutes=6),
}


class SCHISMJSON(ModelJSON, NEMSCapJSON, AttributeJSON):
    """
    SCHISM configuration in ``configure_schism.json``

    stores a number of SCHISM parameters and optionally NEMS parameters
    """

    name = 'SCHISM'
    default_filename = f'configure_schism.json'
    default_processors = 11
    default_attributes = PYSCHISM_ATTRIBUTES

    field_types = {
        'schism_executable_path': Path,
        'schism_hotstart_combiner_path': Path,
        'schism_schout_combiner_path': Path,
        'modeled_start_time': datetime,
        'modeled_end_time': datetime,
        'modeled_timestep': timedelta,
        'fgrid_path': Path,
        'hgrid_path': Path,
        'schism_use_old_io': bool,
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
        #        'output_velocities': bool,
        #        'output_concentrations': bool,
    }

    def __init__(
        self,
        schism_executable_path: PathLike,
        schism_hotstart_combiner_path: PathLike,
        modeled_start_time: datetime,
        modeled_end_time: datetime,
        modeled_timestep: timedelta,
        fgrid_path: PathLike,
        hgrid_path: PathLike,
        schism_use_old_io: bool = False,
        schism_schout_combiner_path: PathLike = None,
        tidal_spinup_duration: timedelta = None,
        tidal_spinup_timestep: timedelta = None,
        forcings: List[ModelForcing] = None,
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
        #        output_concentrations: bool = False,
        #        output_meteorological_factors: bool = False,
        processors: int = 11,
        nems_parameters: Dict[str, str] = None,
        attributes: Dict[str, Any] = None,
        **kwargs,
    ):
        """
        :param schism_executable_path: file path to ``pschism_TVD-VL`` or ``NEMS.x``
        :param schism_hotstart_combiner_path: file path to ``combine_hotstart7``
        :param schism_schout_combiner_path: file path to ``combine_output11``
        :param modeled_start_time: start time in model run
        :param modeled_end_time: edn time in model run
        :param modeled_timestep: time interval between model steps
        :param fgrid_path: file path to bottom friction file
        :param hgrid_path: file path to ``hgrid.gr3``
        :param schism_use_old_io: flag to use old or new schism IO
        :param tidal_spinup_duration: tidal spinup duration for SCHISM coldstart
        :param tidal_spinup_timestep: tidal spinup modeled time interval for SCHISM coldstart
        :param forcings: list of ModelForcing objects to apply to the mesh
        :param source_filename: path to modulefile to ``source``
        :param slurm_configuration: Slurm configuration object
        :param use_original_mesh: whether to symlink / copy original mesh instead of rewriting with ``pyschism``
        :param output_surface: write surface (entire mesh) to NetCDF
        :param surface_output_interval: frequency at which output is written to file
        :param output_stations: write stations outputs timehistory (only applicable if stations file exists)
        :param stations_output_interval: frequency at which stations output is written to file
        :param stations_file_path: file path to stations file
        :param output_spinup: write spinup to NetCDF
        :param output_elevations: write elevations to NetCDF
        :param output_velocities: write velocities to NetCDF
        :param processors: number of processors to use
        :param nems_parameters: parameters to give to NEMS cap
        :param attributes: attributes to set in ``pyschism.Param`` object
        """

        self.__mesh = None
        self.__base_mesh = None

        if tidal_spinup_timestep is None:
            tidal_spinup_timestep = modeled_timestep

        if 'fields' not in kwargs:
            kwargs['fields'] = {}
        kwargs['fields'].update(SCHISMJSON.field_types)

        ModelJSON.__init__(self, model=Model.SCHISM, **kwargs)
        NEMSCapJSON.__init__(
            self, processors=processors, nems_parameters=nems_parameters, **kwargs
        )
        AttributeJSON.__init__(self, attributes=attributes, **kwargs)

        self['schism_executable_path'] = schism_executable_path
        self['schism_hotstart_combiner_path'] = schism_hotstart_combiner_path
        self['schism_schout_combiner_path'] = schism_schout_combiner_path
        self['modeled_start_time'] = modeled_start_time
        self['modeled_end_time'] = modeled_end_time
        self['modeled_timestep'] = modeled_timestep
        self['fgrid_path'] = fgrid_path
        self['hgrid_path'] = hgrid_path
        self['schism_use_old_io'] = schism_use_old_io
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
        #        self['output_concentrations'] = output_concentrations
        #        self['output_meteorological_factors'] = output_meteorological_factors

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
    def slurm_configuration(self) -> List[SlurmJSON]:
        return self.__slurm_configuration

    @slurm_configuration.setter
    def slurm_configuration(self, slurm_configuration: SlurmJSON):
        if isinstance(slurm_configuration, SlurmConfig):
            SlurmJSON.from_pyschism(slurm_configuration)
        self.__slurm_configuration = slurm_configuration

    @property
    def pyschism_forcings(self) -> List[ModelForcing]:
        return [forcing.pyschism_forcing for forcing in self.forcings]

    @property
    def pyschism_mesh(self) -> Hgrid:
        if self.__mesh is None:
            # TODO: PySCHISM copy() for hgrid loses boundary information!
            #            mesh = self.base_mesh.copy()
            mesh = self.base_mesh

            # TODO: reconstruct mesh from base?
            self.__mesh = mesh

        return self.__mesh

    @pyschism_mesh.setter
    def pyschism_mesh(self, pyschism_mesh: Union[Hgrid, PathLike]):
        self.__mesh = pyschism_mesh

    @property
    def base_mesh(self) -> Hgrid:
        if self.__base_mesh is None:
            if self.__mesh is not None:
                self.__base_mesh = self.__mesh
            else:
                self.__base_mesh = self['hgrid_path']
        if not isinstance(self.__base_mesh, Hgrid):
            LOGGER.info(f'opening mesh "{self.__base_mesh}"')
            self.__base_mesh = Hgrid.open(self.__base_mesh, crs=4326)
        # TODO: deconstruct mesh into a base mesh that can be pickled
        return self.__base_mesh

    @base_mesh.setter
    def base_mesh(self, base_mesh: Union[Hgrid, PathLike]):
        self.__base_mesh = base_mesh

    @property
    def pyschism_driver(self) -> ModelDriver:

        tidal_elevation = None
        tidal_velocity = None
        meteo = None
        hydrology = None
        for pyschism_forcing in self.pyschism_forcings:
            if isinstance(pyschism_forcing, Tides):

                # NOTE: Create tidal BC and then replace the tidal database
                tidal_elevation = TidalElevation()
                tidal_elevation.tides = pyschism_forcing
                tidal_velocity = TidalVelocity()
                tidal_velocity.tides = pyschism_forcing

            elif isinstance(pyschism_forcing, NWS):
                meteo = pyschism_forcing

            elif isinstance(pyschism_forcing, NWM):
                hydrology = pyschism_forcing

        config = ModelConfig(
            hgrid=self.pyschism_mesh,
            vgrid=None,
            fgrid=None,  # pyschism writes linear with depth for 2D
            iettype=tidal_elevation,
            ifltype=tidal_velocity,
            nws=meteo,
            source_sink=hydrology,
        )

        # TODO: What about other variable outputs?

        stations = None
        if self['stations_file_path'] is not None:
            stations_output_interval = self['stations_output_interval']
            if stations_output_interval is None:
                stations_output_interval = self['surface_output_interval']

            stations = Stations.from_file(
                file=self['stations_file_path'],
                nspool_sta=stations_output_interval,
                crs=4326,
                elev=self['output_elevations'],
                u=self['output_velocities'],
                v=self['output_velocities'],
            )

        spinup_duration = self['tidal_spinup_duration']
        if spinup_duration is None:
            spinup_duration = timedelta(0)

        # Note we always create coldstart driver, if we want to write
        # hotstart, we just remove ramp attribtues later
        driver = config.coldstart(
            start_date=self['modeled_start_time'],
            end_date=self['modeled_end_time'],
            timestep=self['modeled_timestep'],
            dramp=spinup_duration,
            drampbc=spinup_duration,
            dramp_ss=spinup_duration,
            drampwind=spinup_duration,
            nspool=self['surface_output_interval'],
            elev=self['output_elevations'],
            dahv=self['output_velocities'],
            stations=stations,
            ihfskip=None,  # pyschism sets None to "rnday" value
            nhot_write=None,  # pyschism sets None to "ihfskip" value
        )

        # Add manual overrides to parameters in param.nml
        for name, value in self['attributes'].items():
            if value is not None:
                try:
                    for nml_name in ['core', 'opt', 'schout']:
                        nml_attr = getattr(driver.param, nml_name)
                        if name not in dir(nml_attr):
                            continue

                        setattr(nml_attr, name, value)
                except:
                    LOGGER.warning(
                        f'could not set `{driver.__class__.__name__}` attribute `{name}` to `{value}`'
                    )

        return driver

    @property
    def nemspy_entry(self) -> SCHISMEntry:
        return SCHISMEntry(processors=self['processors'], **self['nems_parameters'])

    def __setitem__(self, key: str, value: Any):
        super().__setitem__(key, value)
        self.__mesh = None

    def __copy__(self) -> 'SCHISMJSON':
        instance = super().__copy__()
        instance.forcings = self.forcings
        return instance
