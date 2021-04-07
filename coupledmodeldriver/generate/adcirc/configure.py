from datetime import datetime, timedelta
from os import PathLike
from pathlib import Path

from adcircpy import AdcircMesh, AdcircRun, Tides
from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing

from coupledmodeldriver.configure.base import ModelDriverJSON, SlurmJSON
from coupledmodeldriver.configure.forcings import (
    ATMESHForcingJSON,
    TidalForcingJSON,
    WW3DATAForcingJSON,
)
from coupledmodeldriver.configure.forcings.base import ForcingJSON
from coupledmodeldriver.configure.models import ADCIRCJSON
from coupledmodeldriver.configure.run import RunConfiguration
from coupledmodeldriver.platforms import Platform


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
        adcirc_executable: PathLike = None,
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
        :param adcirc_executable: filename of compiled `adcirc`
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
            fort_13_path=fort13,
            fort_14_path=fort14,
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
        if not isinstance(forcing, ForcingJSON):
            if isinstance(forcing, AtmosphericMeshForcing):
                forcing = ATMESHForcingJSON.from_adcircpy(forcing)
            elif isinstance(forcing, WaveWatch3DataForcing):
                forcing = WW3DATAForcingJSON.from_adcircpy(forcing)
            elif isinstance(forcing, Tides):
                forcing = TidalForcingJSON.from_adcircpy(forcing)
            else:
                raise NotImplementedError(f'unable to parse object of type {type(forcing)}')

        if forcing not in self:
            self.configurations[forcing.name] = forcing
            self['adcirc'].forcings.append(forcing)

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
