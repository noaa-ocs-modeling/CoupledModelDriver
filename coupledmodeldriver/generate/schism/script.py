from datetime import timedelta
from os import PathLike
from pathlib import Path
from typing import List

from coupledmodeldriver.platforms import Platform
from coupledmodeldriver.script import JobScript


class SchismJob(JobScript):
    """
    abstraction of a job script for running SCHISM
    """

    def __init__(
        self,
        platform: Platform,
        commands: List[str],
        slurm_run_name: str = None,
        slurm_tasks: int = None,
        slurm_duration: timedelta = None,
        slurm_account: str = None,
        source_filename: PathLike = None,
        **kwargs,
    ):
        if slurm_run_name is None:
            slurm_run_name = 'SCHISM_JOB'

        super().__init__(
            platform=platform,
            commands=commands,
            slurm_run_name=slurm_run_name,
            slurm_tasks=slurm_tasks,
            slurm_duration=slurm_duration,
            slurm_account=slurm_account,
            **kwargs,
        )

        if source_filename is not None:
            if not isinstance(source_filename, Path):
                source_filename = Path(source_filename)
            source_filename = source_filename.as_posix()
            if len(str(source_filename)) > 0:
                self.commands.insert(0, f'source {source_filename}')


class SchismRunJob(SchismJob):
    """
    job script for running SCHISM
    """

    def __init__(
        self,
        platform: Platform,
        slurm_run_name: str = None,
        slurm_tasks: int = None,
        slurm_duration: timedelta = None,
        slurm_account: str = None,
        executable: PathLike = None,
        commands: List[str] = None,
        **kwargs,
    ):
        super().__init__(
            platform=platform,
            commands=commands,
            slurm_run_name=slurm_run_name,
            slurm_tasks=slurm_tasks,
            slurm_duration=slurm_duration,
            slurm_account=slurm_account,
            **kwargs,
        )

        if executable is None:
            executable = 'schism'
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
