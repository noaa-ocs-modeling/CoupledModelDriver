from datetime import datetime, timedelta
from os import PathLike
from pathlib import Path
from typing import Any

from adcircpy import AdcircMesh, AdcircRun
from nemspy import ModelingSystem

from coupledmodeldriver.configure.base import (
    ConfigurationJSON,
    ModelDriverJSON,
    NEMSCapJSON,
    NEMSJSON,
    SlurmJSON,
)
from coupledmodeldriver.configure.configure import RunConfiguration
from coupledmodeldriver.configure.forcings.base import (
    ATMESHForcingJSON,
    ForcingJSON,
    TidalForcingJSON,
    WW3DATAForcingJSON,
)
from coupledmodeldriver.generate.adcirc.base import ADCIRCJSON
from coupledmodeldriver.platforms import Platform
from coupledmodeldriver.utilities import LOGGER


class ADCIRCRunConfiguration(RunConfiguration):
    REQUIRED = {
        ModelDriverJSON,
        SlurmJSON,
        ADCIRCJSON,
    }
    SUPPLEMENTARY = {
        TidalForcingJSON,
        ATMESHForcingJSON,
        WW3DATAForcingJSON,
    }

    def __init__(
        self,
        mesh_directory: PathLike,
        modeled_start_time: datetime,
        modeled_end_time: datetime,
        modeled_timestep: timedelta,
        tidal_spinup_duration: timedelta = None,
        platform: Platform = None,
        perturbations: {str: {str: Any}} = None,
        forcings: [ForcingJSON] = None,
        adcirc_processors: int = None,
        slurm_job_duration: timedelta = None,
        slurm_partition: str = None,
        slurm_email_address: str = None,
        adcirc_executable: PathLike = None,
        adcprep_executable: PathLike = None,
        source_filename: PathLike = None,
    ):
        """
        Generate required configuration files for an ADCIRC run.

        :param mesh_directory: path to input mesh directory (containing `fort.13`, `fort.14`)
        :param modeled_start_time: start time within the modeled system
        :param modeled_end_time: end time within the modeled system
        :param modeled_timestep: time interval within the modeled system
        :param adcirc_processors: numbers of processors to assign for ADCIRC
        :param platform: HPC platform for which to configure
        :param tidal_spinup_duration: spinup time for ADCIRC tidal coldstart
        :param perturbations: dictionary of runs encompassing run names to parameter values
        :param forcings: list of forcing configurations to connect to ADCIRC
        :param slurm_job_duration: wall clock time of job
        :param slurm_partition: Slurm partition
        :param slurm_email_address: email address to send Slurm notifications
        :param adcirc_executable: filename of compiled `adcirc`
        :param adcprep_executable: filename of compiled `adcprep`
        :param source_filename: path to module file to `source`
        """

        if not isinstance(mesh_directory, Path):
            mesh_directory = Path(mesh_directory)

        if platform is None:
            platform = Platform.LOCAL

        if forcings is None:
            forcings = []

        if adcirc_processors is None:
            adcirc_processors = 11

        if adcirc_executable is None:
            adcirc_executable = 'adcirc'

        if adcprep_executable is None:
            adcprep_executable = 'adcprep'

        slurm = SlurmJSON(
            account=platform.value['slurm_account'],
            tasks=adcirc_processors,
            partition=slurm_partition,
            job_duration=slurm_job_duration,
            email_address=slurm_email_address,
        )

        adcirc = ADCIRCJSON(
            adcirc_executable_path=adcirc_executable,
            adcprep_executable_path=adcprep_executable,
            modeled_start_time=modeled_start_time,
            modeled_end_time=modeled_end_time,
            modeled_timestep=modeled_timestep,
            fort_13_path=mesh_directory / 'fort.13',
            fort_14_path=mesh_directory / 'fort.14',
            tidal_spinup_duration=tidal_spinup_duration,
            source_filename=source_filename,
            slurm_configuration=slurm,
            processors=adcirc_processors,
        )

        driver = ModelDriverJSON(platform=platform, perturbations=perturbations)
        super().__init__([driver, slurm, adcirc])

        for forcing in forcings:
            self.add_forcing(forcing)

    def add_forcing(self, forcing: ForcingJSON):
        if forcing not in self:
            forcing = self[self.add(forcing)]
            try:
                self['adcirc'].add_forcing(forcing)
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
    def adcircpy_mesh(self) -> AdcircMesh:
        return self['adcirc'].adcircpy_mesh

    @property
    def adcircpy_driver(self) -> AdcircRun:
        return self['adcirc'].adcircpy_driver

    def __copy__(self) -> 'ADCIRCRunConfiguration':
        return self.__class__.from_configurations(self.configurations)

    @classmethod
    def from_configurations(
        cls, configurations: [ConfigurationJSON]
    ) -> 'ADCIRCRunConfiguration':
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
        instance['adcirc'] = required[ADCIRCJSON]

        forcings = [
            configuration
            for configuration in supplementary
            if isinstance(configuration, ForcingJSON)
        ]
        for forcing in forcings:
            instance.add_forcing(forcing)

        return instance

    @classmethod
    def read_directory(
        cls, directory: PathLike, required: [type] = None, supplementary: [type] = None
    ) -> 'ADCIRCRunConfiguration':
        if not isinstance(directory, Path):
            directory = Path(directory)
        if directory.is_file():
            directory = directory.parent
        if required is None:
            required = set()
        required.update(ADCIRCRunConfiguration.REQUIRED)
        if supplementary is None:
            supplementary = set()
        supplementary.update(ADCIRCRunConfiguration.SUPPLEMENTARY)

        return super().read_directory(directory, required, supplementary)


class NEMSADCIRCRunConfiguration(ADCIRCRunConfiguration):
    REQUIRED = {
        ModelDriverJSON,
        NEMSJSON,
        SlurmJSON,
        ADCIRCJSON,
    }

    def __init__(
        self,
        mesh_directory: PathLike,
        modeled_start_time: datetime,
        modeled_end_time: datetime,
        modeled_timestep: timedelta,
        nems_interval: timedelta,
        nems_connections: [str],
        nems_mediations: [str],
        nems_sequence: [str],
        tidal_spinup_duration: timedelta = None,
        platform: Platform = None,
        perturbations: {str: {str: Any}} = None,
        forcings: [ForcingJSON] = None,
        adcirc_processors: int = None,
        slurm_job_duration: timedelta = None,
        slurm_partition: str = None,
        slurm_email_address: str = None,
        nems_executable: PathLike = None,
        adcprep_executable: PathLike = None,
        source_filename: PathLike = None,
    ):
        self.__nems = None

        super().__init__(
            mesh_directory=mesh_directory,
            modeled_start_time=modeled_start_time,
            modeled_end_time=modeled_end_time,
            modeled_timestep=modeled_timestep,
            tidal_spinup_duration=tidal_spinup_duration,
            platform=platform,
            perturbations=perturbations,
            forcings=None,
            adcirc_processors=adcirc_processors,
            slurm_job_duration=slurm_job_duration,
            slurm_partition=slurm_partition,
            slurm_email_address=slurm_email_address,
            adcprep_executable=adcprep_executable,
            source_filename=source_filename,
        )

        nems = NEMSJSON(
            executable_path=nems_executable,
            modeled_start_time=modeled_start_time,
            modeled_end_time=modeled_end_time,
            interval=nems_interval,
            models=self.nemspy_entries,
            connections=nems_connections,
            mediations=nems_mediations,
            sequence=nems_sequence,
        )

        self[nems.name.lower()] = nems

        for forcing in forcings:
            self.add_forcing(forcing)

        self['slurm']['tasks'] = self['nems'].nemspy_modeling_system.processors

    @property
    def nemspy_modeling_system(self) -> ModelingSystem:
        return self['nems'].nemspy_modeling_system

    def add_forcing(self, forcing: ForcingJSON):
        if forcing not in self:
            forcing = self[self.add(forcing)]
            if isinstance(forcing, NEMSCapJSON):
                self['nems']['models'].append(forcing.nemspy_entry)
            self['adcirc'].add_forcing(forcing)

    def __copy__(self) -> 'NEMSADCIRCRunConfiguration':
        return self.__class__.from_configurations(self.configurations)

    @classmethod
    def from_configurations(
        cls, configurations: [ConfigurationJSON]
    ) -> 'NEMSADCIRCRunConfiguration':
        instance = ADCIRCRunConfiguration.from_configurations(configurations)
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
    ) -> 'NEMSADCIRCRunConfiguration':
        if not isinstance(directory, Path):
            directory = Path(directory)
        if directory.is_file():
            directory = directory.parent
        if required is None:
            required = set()
        required.update(NEMSADCIRCRunConfiguration.REQUIRED)
        if supplementary is None:
            supplementary = set()
        supplementary.update(NEMSADCIRCRunConfiguration.SUPPLEMENTARY)

        return super().read_directory(directory, required, supplementary)
