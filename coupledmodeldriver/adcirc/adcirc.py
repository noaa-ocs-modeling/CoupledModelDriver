from datetime import datetime, timedelta
from os import PathLike
from pathlib import Path

from adcircpy import AdcircMesh, AdcircRun, Tides
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing

from ..configuration import (
    ADCIRCJSON,
    ATMESHForcingJSON,
    ForcingJSON,
    ModelDriverJSON,
    RunConfiguration,
    SlurmJSON,
    TidalForcingJSON,
    WW3DATAForcingJSON,
)
from ..platforms import Platform


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
        fort13: PathLike,
        fort14: PathLike,
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
        adcprep_executable: PathLike = None,
        source_filename: PathLike = None,
    ):
        """
        Generate required configuration files for an ADCIRC run.

        :param fort13: path to input mesh values (`fort.13`)
        :param fort14: path to input mesh nodes (`fort.14`)
        :param modeled_start_time: start time within the modeled system
        :param modeled_end_time: end time within the modeled system
        :param modeled_timestep: time interval within the modeled system
        :param adcirc_processors: numbers of processors to use for Slurm job
        :param platform: HPC platform for which to configure
        :param tidal_spinup_duration: spinup time for ADCIRC coldstart
        :param runs: dictionary of run name to run value and mesh attribute name
        :param slurm_job_duration: wall clock time of job
        :param slurm_partition: Slurm partition
        :param slurm_email_address: email address to send Slurm notifications
        :param adcprep_executable: filename of compiled `adcprep`
        :param source_filename: path to module file to `source`
        """

        if platform is None:
            platform = Platform.LOCAL

        if forcings is None:
            forcings = []

        if adcirc_processors is None:
            adcirc_processors = 11

        if adcprep_executable is None:
            adcprep_executable = 'adcprep'

        self.__slurm = SlurmJSON(
            account=platform.value['slurm_account'],
            tasks=adcirc_processors,
            partition=slurm_partition,
            job_duration=slurm_job_duration,
            email_address=slurm_email_address,
        )

        self.__adcirc = ADCIRCJSON(
            adcprep_executable_path=adcprep_executable,
            modeled_start_time=modeled_start_time,
            modeled_end_time=modeled_end_time,
            modeled_timestep=modeled_timestep,
            fort_13_path=fort13,
            fort_14_path=fort14,
            tidal_spinup_duration=tidal_spinup_duration,
            source_filename=source_filename,
            slurm_configuration=self.__slurm,
            processors=adcirc_processors,
        )

        self.__driver = ModelDriverJSON(platform=platform, runs=runs)
        super().__init__([self.__driver, self.__slurm, self.__adcirc])

        for forcing in forcings:
            self.add_forcing(forcing)

    @property
    def driver(self) -> ModelDriverJSON:
        return self.__driver

    @property
    def slurm(self) -> SlurmJSON:
        return self.__slurm

    @property
    def adcirc(self) -> ADCIRCJSON:
        return self.__adcirc

    def add_forcing(self, forcing: ForcingJSON):
        if not isinstance(forcing, ForcingJSON):
            if isinstance(forcing, AtmosphericMeshForcing):
                forcing = ATMESHForcingJSON.from_adcircpy(forcing)
            elif isinstance(forcing, WaveWatch3DataForcing):
                forcing = WW3DATAForcingJSON.from_adcircpy(forcing)
            elif isinstance(forcing, Tides):
                forcing = TidalForcingJSON.from_adcircpy(forcing)
            else:
                raise NotImplementedError(f'unable to parse object of type {type(forcing)}')

        if forcing not in self.configurations:
            self.configurations[forcing.name] = forcing
            self.adcirc.forcings.append(forcing)

    @property
    def adcircpy_mesh(self) -> AdcircMesh:
        return self.adcirc.adcircpy_mesh

    @property
    def adcircpy_driver(self) -> AdcircRun:
        return self.adcirc.adcircpy_driver

    @classmethod
    def read_directory(cls, directory: PathLike) -> 'RunConfiguration':
        if not isinstance(directory, Path):
            directory = Path(directory)
        if directory.is_file():
            directory = directory.parent

        configurations = []
        for name, configuration_class in cls.required:
            filename = directory / configuration_class.default_filename
            if filename.exists():
                configurations.append(configuration_class.from_file(filename))
            else:
                raise FileNotFoundError(f'missing required configuration file "{filename}"')

        driver = configurations[0]
        slurm = configurations[1]
        adcirc = configurations[2]

        instance = cls(
            fort13=adcirc['fort_13_path'],
            fort14=adcirc['fort_14_path'],
            modeled_start_time=adcirc['modeled_start_time'],
            modeled_end_time=adcirc['modeled_end_time'],
            modeled_timestep=adcirc['modeled_timestep'],
            tidal_spinup_duration=adcirc['tidal_spinup_duration'],
            platform=driver['platform'],
            runs=driver['runs'],
            slurm_processors=slurm['tasks'],
            slurm_job_duration=slurm['job_duration'],
            slurm_partition=slurm['partition'],
            slurm_email_address=slurm['email_address'],
            adcprep_executable=adcirc['adcprep_executable_path'],
            source_filename=adcirc['source_filename'],
        )

        for name, configuration_class in cls.forcings:
            filename = directory / configuration_class.default_filename
            if filename.exists():
                instance.add_forcing(configuration_class.from_file(filename))

        return instance
