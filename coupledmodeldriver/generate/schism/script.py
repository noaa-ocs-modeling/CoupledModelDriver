from datetime import timedelta
from os import PathLike
from pathlib import Path
from typing import List

from coupledmodeldriver.platforms import Platform
from coupledmodeldriver.script import (
    JobScript,
    EnsembleGenerationJob,
    EnsembleRunScript,
    EnsembleCleanupScript,
    slurm_dependencies,
    slurm_submit_get_id,
    bash_if,
)


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
        num_strides: int = 4,
        old_io: bool = False,
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
            executable = 'pschism-TVD_VL'
        else:
            if not isinstance(executable, Path):
                executable = Path(executable)
            executable = executable.as_posix()

        self.executable = executable
        if old_io:
            self.commands.append(
                f'{self.launcher} {self.executable}'
                if self.launcher is not None
                else f'{self.executable}'
            )
        else:
            self.commands.append(
                f'{self.launcher} {self.executable} {num_strides}'
                if self.launcher is not None
                else f'{self.executable} {num_strides}'
            )


class SchismCombineSchoutJob(SchismJob):
    def __init__(
        self,
        platform: Platform,
        start_idx: int,
        end_idx: int,
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
            executable = 'combine_output11'
        else:
            if not isinstance(executable, Path):
                executable = Path(executable)
            executable = executable.as_posix()

        self.executable = executable
        self.start_idx = start_idx
        self.end_idx = end_idx
        self.commands.extend(
            [
                'pushd outputs',
                f'{self.executable} --begin {self.start_idx} --end {self.end_idx}',
                'popd',
            ]
        )


class SchismCombineHotstartJob(SchismJob):
    def __init__(
        self,
        platform: Platform,
        iterations_idx: List[int],
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
            executable = 'combine_hotstart7'
        else:
            if not isinstance(executable, Path):
                executable = Path(executable)
            executable = executable.as_posix()

        self.executable = executable
        self.iterations_idx = iterations_idx
        self.commands.extend(
            [
                'pushd outputs',
                *[f'{self.executable} --iteration {it}' for it in self.iterations_idx],
                'popd',
            ]
        )


class SchismEnsembleGenerationJob(EnsembleGenerationJob):
    """
    job script to generate the ensemble configuration
    """

    def __init__(
        self,
        platform: Platform,
        slurm_tasks: int = None,
        slurm_duration: timedelta = None,
        slurm_account: str = None,
        commands: List[str] = None,
        parallel: bool = False,
        **kwargs,
    ):
        super().__init__(
            platform=platform,
            generate_command='generate_schism',
            commands=commands,
            slurm_run_name='SCHISM_GENERATE_CONFIGURATION',
            slurm_tasks=slurm_tasks,
            slurm_duration=slurm_duration,
            slurm_account=slurm_account,
            parallel=parallel,
            **kwargs,
        )


class SchismEnsembleRunScript(EnsembleRunScript):
    """
    script to run the ensemble, either by running it directly or by submitting model execution to the job manager
    default filename is ``run_<platform>.sh``
    """

    def _spinup_lines(self):
        spinup_lines = ['# run spinup', 'pushd ${DIRECTORY}/spinup >/dev/null 2>&1']
        if self.platform.value['uses_slurm']:
            spinup_lines.extend(
                [
                    slurm_submit_get_id('schism.job', 'spinup_jobid'),
                    slurm_submit_get_id(
                        'combine_hotstart.job',
                        'combine_hotstart_jobid',
                        dependencies=slurm_dependencies(after_ok=['$spinup_jobid']),
                    ),
                ]
            )
        else:
            spinup_lines.extend(['sh schism.job', 'sh combine_hotstart.job'])
        spinup_lines.extend(['popd >/dev/null 2>&1', ''])

        return spinup_lines

    def _hotstart_lines(self):
        hotstart_lines = ['pushd ${hotstart} >/dev/null 2>&1']
        if self.platform.value['uses_slurm']:
            after_ok = []
            if self.run_spinup:
                after_ok.append('$combine_hotstart_jobid')
            dependencies = slurm_dependencies(after_ok=after_ok)
            hotstart_lines.extend(
                [
                    slurm_submit_get_id('schism.job', 'run_jobid', dependencies=dependencies),
                    bash_if(
                        '[ -f combine_hotstart.job ]',
                        slurm_submit_get_id(
                            'combine_hotstart.job',
                            '_',
                            dependencies=slurm_dependencies(after_ok=['$run_jobid']),
                        ),
                    ),
                    bash_if(
                        '[ -f combine_output.job ]',
                        slurm_submit_get_id(
                            'combine_output.job',
                            '_',
                            dependencies=slurm_dependencies(after_ok=['$run_jobid']),
                        ),
                    ),
                ]
            )
        else:
            hotstart_lines.extend(
                [
                    'sh schism.job',
                    bash_if('[ -f combine_hotstart.job ]', 'sh combine_hotstart.job &'),
                    bash_if('[ -f combine_output.job ]', 'sh combine_output.job'),
                ]
            )
        hotstart_lines.append('popd >/dev/null 2>&1')

        return hotstart_lines


class SchismEnsembleCleanupScript(EnsembleCleanupScript):
    """
    script for cleaning an ensemble configuration, by deleting output and log files
    """

    def __init__(self, commands: List[str] = None):
        filenames = ['outputs/*']
        spinup_filenames = []
        hotstart_filenames = []

        super().__init__(commands, filenames, spinup_filenames, hotstart_filenames)
