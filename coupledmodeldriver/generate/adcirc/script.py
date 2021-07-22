from datetime import timedelta
from os import PathLike
from pathlib import Path

from coupledmodeldriver.platforms import Platform
from coupledmodeldriver.script import JobScript


class AdcircJob(JobScript):
    def __init__(
        self,
        platform: Platform,
        commands: [str],
        slurm_tasks: int,
        slurm_account: str,
        slurm_duration: timedelta,
        slurm_run_name: str,
        source_filename: PathLike = None,
        **kwargs,
    ):
        if slurm_run_name is None:
            slurm_run_name = 'ADCIRC_JOB'

        super().__init__(
            platform,
            commands,
            slurm_tasks,
            slurm_account,
            slurm_duration,
            slurm_run_name,
            **kwargs,
        )

        if source_filename is not None:
            if not isinstance(source_filename, Path):
                source_filename = Path(source_filename)
            source_filename = source_filename.as_posix()
            if len(str(source_filename)) > 0:
                self.commands.insert(0, f'source {source_filename}')


class AdcircRunJob(AdcircJob):
    """ script for running ADCIRC via a NEMS configuration """

    def __init__(
        self,
        platform: Platform,
        slurm_tasks: int,
        slurm_account: str,
        slurm_duration: timedelta,
        slurm_run_name: str,
        executable: PathLike = None,
        commands: [str] = None,
        **kwargs,
    ):
        super().__init__(
            platform,
            commands,
            slurm_tasks,
            slurm_account,
            slurm_duration,
            slurm_run_name,
            **kwargs,
        )

        if executable is None:
            executable = 'adcirc'
        else:
            if not isinstance(executable, Path):
                executable = Path(executable)
            executable = executable.as_posix()

        self.executable = executable
        self.commands.append(
            f'{self.launcher} {self.executable}'
            if self.launcher is not None
            else f'{self.executable}'
        )


class AswipCommand:
    """ implement guidance and documentation from @WPringle """

    def __init__(
        self,
        path: PathLike,
        nws: int = None,
        isotachs: int = None,
        rmax_approaches: int = None,
    ):
        self.__path = None
        self.__nws = None
        self.__isotachs = None
        self.__rmax_approaches = None

        self.path = path
        self.nws = nws
        self.isotachs = isotachs
        self.rmax_approaches = rmax_approaches

    @property
    def path(self) -> Path:
        """ path to `aswip` executable  """
        return self.__path

    @path.setter
    def path(self, path: PathLike):
        if not isinstance(path, Path):
            path = Path(path)

        self.__path = path

    @property
    def nws(self) -> int:
        """ `-n` - ADCIRC NWS option """
        return self.__nws

    @nws.setter
    def nws(self, nws: int):
        self.__nws = nws

    @property
    def isotachs(self) -> int:
        """
        `-m` - methods using isotachs, one of [1, 2, 3, 4]

        1 = use the 34 isotach,
        2 = use the 64 isotach,
        3 = use the 50 isotach,
        4 = use all available isotachs (use 4 for NWS=20)
        """

        if self.__isotachs is None:
            if self.nws == 20:
                self.__isotachs = 4

        return self.__isotachs

    @isotachs.setter
    def isotachs(self, isotachs: int):
        self.__isotachs = isotachs

    @property
    def rmax_approaches(self) -> int:
        """
        `-z` - approaches solving for Rmax, one of [1, 2]

        1 = only rotate wind vectors afterward,
        2 = rotate wind vectors before and afterwards (use this for NWS=20)
        """

        if self.__rmax_approaches is None:
            if self.nws == 20:
                self.__rmax_approaches = 2

        return self.__rmax_approaches

    @rmax_approaches.setter
    def rmax_approaches(self, rmax_approaches: int):
        self.__rmax_approaches = rmax_approaches

    def __str__(self) -> str:
        return '\n'.join(
            [
                f'{self.path.as_posix()} -n {self.nws} -m {self.isotachs} -z {self.rmax_approaches}',
                'mv fort.22 fort.22.original',
                'mv NWS_20_fort.22 fort.22',
            ]
        )

    @classmethod
    def from_string(cls, aswip_command: str) -> 'AswipCommand':
        parts = aswip_command.split()[0]
        path = parts[0]

        if '-n' in parts:
            nws = parts[parts.index('-n') + 1]
        else:
            nws = None

        if '-m' in parts:
            isotachs = parts[parts.index('-m') + 1]
        else:
            isotachs = None

        if '-z' in parts:
            rmax_approaches = parts[parts.index('-z') + 1]
        else:
            rmax_approaches = None

        return cls(path=path, nws=nws, isotachs=isotachs, rmax_approaches=rmax_approaches)


class AdcircSetupJob(AdcircJob):
    """ script for performing domain decomposition with `adcprep` """

    def __init__(
        self,
        platform: Platform,
        adcirc_mesh_partitions: int,
        slurm_account: str,
        slurm_duration: timedelta,
        slurm_run_name: str,
        adcprep_path: PathLike = None,
        aswip_command: str = None,
        slurm_tasks: int = 1,
        commands: [str] = None,
        **kwargs,
    ):
        super().__init__(
            platform,
            commands,
            slurm_tasks,
            slurm_account,
            slurm_duration,
            slurm_run_name,
            **kwargs,
        )

        self.adcirc_partitions = adcirc_mesh_partitions

        if adcprep_path is None:
            adcprep_path = 'adcprep'
        else:
            if not isinstance(adcprep_path, Path):
                adcprep_path = Path(adcprep_path)
            adcprep_path = adcprep_path.as_posix()

        if aswip_command is not None:
            if isinstance(aswip_command, str):
                aswip_command = AswipCommand.from_string(aswip_command)

        self.adcprep_path = adcprep_path
        self.aswip_command = aswip_command

        setup_commands = []

        adcprep_commands = [
            f'{self.adcprep_path} --np {self.adcirc_partitions} --partmesh',
            f'{self.adcprep_path} --np {self.adcirc_partitions} --prepall',
        ]
        if self.launcher is not None:
            adcprep_commands = [f'{self.launcher} {line}' for line in adcprep_commands]
        setup_commands.extend(adcprep_commands)

        if self.aswip_command is not None:
            setup_commands.extend(
                ['', '## make sure ATCF format is correct for GAHM', f'{self.aswip_command}']
            )

        self.commands.extend(setup_commands)
