from datetime import datetime, timedelta
from os import PathLike
from pathlib import Path
from typing import Any

from nemspy import ModelingSystem
from pyschism import ModelDomain, ModelDriver
from pyschism.enums import Stratification

from coupledmodeldriver.configure import NEMSJSON
from coupledmodeldriver.configure.base import (
    ConfigurationJSON,
    ModelDriverJSON,
    NEMSCapJSON,
    SlurmJSON,
)
from coupledmodeldriver.configure.configure import RunConfiguration
from coupledmodeldriver.configure.forcings.base import (
    ATMESHForcingJSON,
    BestTrackForcingJSON,
    ForcingJSON,
    OWIForcingJSON,
    TidalForcingJSON,
    WW3DATAForcingJSON,
)
from coupledmodeldriver.generate.schism.base import SCHISMJSON
from coupledmodeldriver.platforms import Platform
from coupledmodeldriver.utilities import LOGGER


class SCHISMRunConfiguration(RunConfiguration):
    REQUIRED = {
        ModelDriverJSON,
        SlurmJSON,
        SCHISMJSON,
    }
    SUPPLEMENTARY = {
        TidalForcingJSON,
        BestTrackForcingJSON,
        OWIForcingJSON,
        ATMESHForcingJSON,
        WW3DATAForcingJSON,
    }

    def __init__(
        self,
        hgrid_path: PathLike,
        vgrid_path: PathLike,
        fgrid_path: PathLike,
        modeled_start_time: datetime,
        modeled_duration: timedelta,
        modeled_timestep: timedelta,
        tidal_spinup_duration: timedelta = None,
        tidal_spinup_duration_bc: timedelta = None,
        stratification: Stratification = None,
        platform: Platform = None,
        perturbations: {str: {str: Any}} = None,
        forcings: [ForcingJSON] = None,
        schism_processors: int = None,
        slurm_job_duration: timedelta = None,
        slurm_partition: str = None,
        slurm_email_address: str = None,
        schism_executable: PathLike = None,
        source_filename: PathLike = None,
    ):
        """
        Generate required configuration files for an SCHISM run.

        :param hgrid_path: path to input horizontal grid
        :param vgrid_path: path to input vertical grid
        :param fgrid_path: path to input friction grid
        :param modeled_start_time: start time within the modeled system
        :param modeled_duration: duration within the modeled system
        :param modeled_timestep: time interval within the modeled system
        :param stratification:
        :param platform: HPC platform for which to configure
        :param tidal_spinup_duration: spinup time for SCHISM tidal coldstart
        :param tidal_spinup_duration_bc: spinup time for SCHISM tidal coldstart
        :param perturbations: dictionary of runs encompassing run names to parameter values
        :param forcings: list of forcing configurations to connect to SCHISM
        :param schism_processors: numbers of processors to assign for SCHISM
        :param slurm_job_duration: wall clock time of job
        :param slurm_partition: Slurm partition
        :param slurm_email_address: email address to send Slurm notifications
        :param schism_executable: filename of compiled `schism`
        :param source_filename: path to module file to `source`
        """

        if not isinstance(hgrid_path, Path):
            hgrid_path = Path(hgrid_path)

        if not isinstance(vgrid_path, Path):
            vgrid_path = Path(vgrid_path)

        if not isinstance(fgrid_path, Path):
            fgrid_path = Path(fgrid_path)

        if platform is None:
            platform = Platform.LOCAL

        if forcings is None:
            forcings = []

        if schism_processors is None:
            schism_processors = 11

        if schism_executable is None:
            schism_executable = 'pschism_TVD-VL'

        slurm = SlurmJSON(
            account=platform.value['slurm_account'],
            tasks=schism_processors,
            partition=slurm_partition,
            job_duration=slurm_job_duration,
            email_address=slurm_email_address,
            extra_commands=[f'source {source_filename}'],
        )

        model = SCHISMJSON(
            mesh_files=[hgrid_path, vgrid_path, fgrid_path],
            executable=schism_executable,
            modeled_start_time=modeled_start_time,
            modeled_duration=modeled_duration,
            modeled_timestep=modeled_timestep,
            tidal_spinup_duration=tidal_spinup_duration,
            tidal_bc_spinup_duration=tidal_spinup_duration_bc,
            tidal_bc_cutoff_depth=None,
            stratification=stratification,
            hotstart_output_interval=None,
            slurm_configuration=slurm,
            hotstart_combination_executable=None,
            surface_output_new_file_skips=None,
            surface_output_variables=None,
            stations_output_interval=None,
            stations_file_path=None,
            stations_crs=None,
            output_frequency=None,
        )

        driver = ModelDriverJSON(platform=platform, perturbations=perturbations)
        super().__init__([driver, slurm, model])

        for forcing in forcings:
            self.add_forcing(forcing)

    def add_forcing(self, forcing: ForcingJSON):
        if forcing not in self:
            forcing = self[self.add(forcing)]
            try:
                self['schism'].add_forcing(forcing)
            except Exception as error:
                LOGGER.error(error)

    @property
    def forcings(self) -> [ForcingJSON]:
        return [
            configuration
            for configuration in self.configurations
            if isinstance(configuration, ForcingJSON)
        ]

    @property
    def pyschism_domain(self) -> ModelDomain:
        return self['schism'].pyschism_domain

    @property
    def pyschism_driver(self) -> ModelDriver:
        return self['schism'].pyschism_driver

    def __copy__(self) -> 'SCHISMRunConfiguration':
        return self.__class__.from_configurations(self.configurations)

    @classmethod
    def from_configurations(
        cls, configurations: [ConfigurationJSON]
    ) -> 'SCHISMRunConfiguration':
        required = {configuration_class: None for configuration_class in cls.REQUIRED}
        supplementary = {
            configuration_class: None for configuration_class in cls.SUPPLEMENTARY
        }

        for configuration in configurations:
            for configuration_class in required:
                if isinstance(configuration, configuration_class):
                    if required[configuration_class] is None:
                        required[configuration_class] = configuration
                        break
                    else:
                        raise ValueError(
                            f'multiple configurations given for "{configuration_class.__name__}"'
                        )
            for configuration_class in supplementary:
                if isinstance(configuration, configuration_class):
                    supplementary[configuration_class] = configuration

        instance = RunConfiguration(required.values())
        instance.__class__ = cls

        instance['modeldriver'] = required[ModelDriverJSON]
        instance['slurm'] = required[SlurmJSON]
        instance['schism'] = required[SCHISMJSON]

        forcings = [
            configuration
            for configuration in supplementary.values()
            if isinstance(configuration, ForcingJSON)
        ]
        for forcing in forcings:
            instance.add_forcing(forcing)

        return instance

    @classmethod
    def read_directory(
        cls, directory: PathLike, required: [type] = None, supplementary: [type] = None
    ) -> 'SCHISMRunConfiguration':
        if not isinstance(directory, Path):
            directory = Path(directory)
        if directory.is_file():
            directory = directory.parent
        if required is None:
            required = set()
        required.update(SCHISMRunConfiguration.REQUIRED)
        if supplementary is None:
            supplementary = set()
        supplementary.update(SCHISMRunConfiguration.SUPPLEMENTARY)

        return super().read_directory(directory, required, supplementary)


class NEMSSCHISMRunConfiguration(SCHISMRunConfiguration):
    REQUIRED = {
        ModelDriverJSON,
        NEMSJSON,
        SlurmJSON,
        SCHISMJSON,
    }

    def __init__(
        self,
        hgrid_path: PathLike,
        vgrid_path: PathLike,
        fgrid_path: PathLike,
        modeled_start_time: datetime,
        modeled_duration: timedelta,
        modeled_timestep: timedelta,
        nems_interval: timedelta,
        nems_connections: [str],
        nems_mediations: [str],
        nems_sequence: [str],
        tidal_spinup_duration: timedelta = None,
        tidal_spinup_duration_bc: timedelta = None,
        stratification: Stratification = None,
        platform: Platform = None,
        perturbations: {str: {str: Any}} = None,
        forcings: [ForcingJSON] = None,
        schism_processors: int = None,
        slurm_job_duration: timedelta = None,
        slurm_partition: str = None,
        slurm_email_address: str = None,
        nems_executable: PathLike = None,
        source_filename: PathLike = None,
    ):
        self.__nems = None

        super().__init__(
            hgrid_path=hgrid_path,
            vgrid_path=vgrid_path,
            fgrid_path=fgrid_path,
            modeled_start_time=modeled_start_time,
            modeled_duration=modeled_duration,
            modeled_timestep=modeled_timestep,
            tidal_spinup_duration=tidal_spinup_duration,
            tidal_spinup_duration_bc=tidal_spinup_duration_bc,
            stratification=stratification,
            platform=platform,
            perturbations=perturbations,
            forcings=None,
            schism_processors=schism_processors,
            slurm_job_duration=slurm_job_duration,
            slurm_partition=slurm_partition,
            slurm_email_address=slurm_email_address,
            schism_executable=None,
            source_filename=source_filename,
        )

        nems = NEMSJSON(
            executable=nems_executable,
            modeled_start_time=modeled_start_time,
            modeled_duration=modeled_duration,
            interval=nems_interval,
            models=self.nemspy_entries,
            connections=nems_connections,
            mediations=nems_mediations,
            sequence=nems_sequence,
        )

        self[nems.name.lower()] = nems

        for forcing in forcings:
            self.add_forcing(forcing)

        self['slurm'].tasks = self['nems'].nemspy_modeling_system.processors

    @property
    def nemspy_modeling_system(self) -> ModelingSystem:
        return self['nems'].nemspy_modeling_system

    def add_forcing(self, forcing: ForcingJSON):
        if forcing not in self:
            forcing = self[self.add(forcing)]
            if isinstance(forcing, NEMSCapJSON):
                self['nems']['models'].append(forcing.nemspy_entry)
            self['schism'].add_forcing(forcing)

    def __copy__(self) -> 'NEMSSCHISMRunConfiguration':
        return self.__class__.from_configurations(self.configurations)

    @classmethod
    def from_configurations(
        cls, configurations: [ConfigurationJSON]
    ) -> 'NEMSSCHISMRunConfiguration':
        instance = SCHISMRunConfiguration.from_configurations(configurations)
        instance.__class__ = cls

        nems = None
        for configuration in configurations:
            if isinstance(configuration, NEMSJSON):
                if nems is None:
                    nems = configuration
                    break
                else:
                    raise ValueError(
                        f'multiple configurations given for "{NEMSJSON.__name__}"'
                    )
        instance['nems'] = nems

        return instance

    @classmethod
    def read_directory(
        cls, directory: PathLike, required: [type] = None, supplementary: [type] = None
    ) -> 'NEMSSCHISMRunConfiguration':
        if not isinstance(directory, Path):
            directory = Path(directory)
        if directory.is_file():
            directory = directory.parent
        if required is None:
            required = set()
        required.update(NEMSSCHISMRunConfiguration.REQUIRED)
        if supplementary is None:
            supplementary = set()
        supplementary.update(NEMSSCHISMRunConfiguration.SUPPLEMENTARY)

        instance = super().read_directory(directory, required, supplementary)
        instance['nems']['models'] = instance.nemspy_entries
        return instance
