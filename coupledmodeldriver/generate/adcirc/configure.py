from datetime import datetime, timedelta
from os import PathLike
from pathlib import Path

from adcircpy import AdcircMesh, AdcircRun
from adcircpy.forcing.base import Forcing
from nemspy import ModelingSystem

from ...configure.base import ModelDriverJSON, NEMSCapJSON, NEMSJSON, \
    SlurmJSON
from ...configure.configure import RunConfiguration
from ...configure.forcings.base import (
    ATMESHForcingJSON,
    ForcingJSON,
    TidalForcingJSON,
    WW3DATAForcingJSON,
)
from ...configure.models import ADCIRCJSON
from ...platforms import Platform
from ...utilities import LOGGER


class ADCIRCRunConfiguration(RunConfiguration):
    required = [
        ModelDriverJSON,
        SlurmJSON,
        ADCIRCJSON,
    ]
    forcings = [
        TidalForcingJSON,
        ATMESHForcingJSON,
        WW3DATAForcingJSON,
    ]

    def __init__(
        self,
        mesh_directory: PathLike,
        modeled_start_time: datetime,
        modeled_end_time: datetime,
        modeled_timestep: timedelta,
        tidal_spinup_duration: timedelta = None,
        platform: Platform = None,
        runs: {str: (float, str)} = None,
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
        :param runs: dictionary of run name to run value and mesh attribute name
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

        driver = ModelDriverJSON(platform=platform, runs=runs)
        super().__init__([driver, slurm, adcirc])

        for forcing in forcings:
            self.add_forcing(forcing)

    def add_forcing(self, forcing: ForcingJSON):
        if forcing not in self:
            name = self.add(forcing)
            try:
                self['adcirc'].add_forcing(self[name])
            except Exception as error:
                LOGGER.error(error)

    @property
    def adcircpy_mesh(self) -> AdcircMesh:
        return self['adcirc'].adcircpy_mesh

    @property
    def adcircpy_driver(self) -> AdcircRun:
        return self['adcirc'].adcircpy_driver

    @classmethod
    def from_configurations(
        cls,
        driver: ModelDriverJSON,
        slurm: SlurmJSON,
        adcirc: ADCIRCJSON,
        forcings: [ForcingJSON] = None,
    ) -> 'ADCIRCRunConfiguration':
        instance = RunConfiguration([driver, slurm, adcirc])
        instance.__class__ = cls

        instance['modeldriver'] = driver
        instance['slurm'] = slurm
        instance['adcirc'] = adcirc

        if forcings is not None:
            for forcing in forcings:
                instance.add_forcing(forcing)

        return instance

    @classmethod
    def read_directory(cls, directory: PathLike) -> 'ADCIRCRunConfiguration':
        if not isinstance(directory, Path):
            directory = Path(directory)
        if directory.is_file():
            directory = directory.parent

        configurations = []
        for configuration_class in cls.required:
            filename = directory / configuration_class.default_filename
            if filename.exists():
                configurations.append(configuration_class.from_file(filename))
            else:
                raise FileNotFoundError(f'missing required configuration file "{filename}"')

        forcings = []
        for configuration_class in cls.forcings:
            filename = directory / configuration_class.default_filename
            if filename.exists():
                forcings.append(configuration_class.from_file(filename))

        return cls.from_configurations(
            driver=configurations[0],
            slurm=configurations[1],
            adcirc=configurations[2],
            forcings=forcings,
        )


class NEMSADCIRCRunConfiguration(ADCIRCRunConfiguration):
    required = [
        ModelDriverJSON,
        NEMSJSON,
        SlurmJSON,
        ADCIRCJSON,
    ]

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
        runs: {str: (float, str)} = None,
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
            runs=runs,
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

        self[nems.name] = nems

        for forcing in forcings:
            self.add_forcing(forcing)

        self['slurm']['tasks'] = self['nems'].nemspy_modeling_system.processors

    @property
    def nemspy_modeling_system(self) -> ModelingSystem:
        return self['nems'].nemspy_modeling_system

    def add_forcing(self, forcing: Forcing):
        if forcing not in self:
            name = self.add(forcing)
            forcing = self[name]
            if isinstance(forcing, NEMSCapJSON):
                self['nems']['models'].append(forcing.nemspy_entry)
            self['adcirc'].add_forcing(forcing)

    @classmethod
    def from_configurations(
        cls,
        driver: ModelDriverJSON,
        nems: NEMSJSON,
        slurm: SlurmJSON,
        adcirc: ADCIRCJSON,
        forcings: [ForcingJSON] = None,
    ) -> 'NEMSADCIRCRunConfiguration':
        instance = super().from_configurations(
            driver=driver, slurm=slurm, adcirc=adcirc, forcings=None,
        )
        instance.__class__ = cls
        instance[nems.name.lower()] = nems

        if forcings is not None:
            for forcing in forcings:
                instance.add_forcing(forcing)

        return instance

    @classmethod
    def read_directory(cls, directory: PathLike) -> 'NEMSADCIRCRunConfiguration':
        if not isinstance(directory, Path):
            directory = Path(directory)
        if directory.is_file():
            directory = directory.parent

        configurations = []
        for configuration_class in cls.required:
            filename = directory / configuration_class.default_filename
            if filename.exists():
                configurations.append(configuration_class.from_file(filename))
            else:
                raise FileNotFoundError(f'missing required configuration file "{filename}"')

        driver, nems, slurm, adcirc = configurations

        forcings = []
        for configuration_class in cls.forcings:
            filename = directory / configuration_class.default_filename
            if filename.exists():
                forcings.append(configuration_class.from_file(filename))

        nems['models'] = [
            entry.nemspy_entry
            for entry in (adcirc, *forcings)
            if isinstance(entry, NEMSCapJSON)
        ]

        return cls.from_configurations(
            driver=driver, nems=nems, slurm=slurm, adcirc=adcirc, forcings=forcings,
        )
