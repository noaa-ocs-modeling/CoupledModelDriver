from abc import ABC, abstractmethod
from datetime import timedelta
from enum import Enum
from os import PathLike
from pathlib import Path, PurePosixPath
import textwrap
from typing import Sequence
import uuid

import numpy


class SlurmEmailType(Enum):
    NONE = 'NONE'
    BEGIN = 'BEGIN'
    END = 'END'
    FAIL = 'FAIL'
    REQUEUE = 'REQUEUE'
    STAGE_OUT = 'STAGE_OUT'  # burst buffer stage out and teardown completed
    ALL = 'ALL'  # equivalent to BEGIN, END, FAIL, REQUEUE, and STAGE_OUT)
    TIME_LIMIT = 'TIME_LIMIT'
    TIME_LIMIT_90 = 'TIME_LIMIT_90'  # reached 90 percent of time limit
    TIME_LIMIT_80 = 'TIME_LIMIT_80'  # reached 80 percent of time limit
    TIME_LIMIT_50 = 'TIME_LIMIT_50'  # reached 50 percent of time limit
    ARRAY_TASKS = 'ARRAY_TASKS'  # send emails for each array task


class Platform(Enum):
    LOCAL = 'local'
    STAMPEDE2 = 'stampede2'
    ORION = 'orion'
    HERA = 'hera'


class Script(ABC):
    shebang = '#!/bin/bash --login'

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def write(self, filename: PathLike, overwrite: bool = False):
        raise NotImplementedError


class JobScript(Script):
    shebang = '#!/bin/bash --login'

    def __init__(
        self,
        platform: Platform,
        commands: [str],
        slurm_tasks: int,
        slurm_account: str,
        slurm_duration: timedelta,
        slurm_run_name: str = None,
        slurm_email_type: SlurmEmailType = None,
        slurm_email_address: str = None,
        slurm_error_filename: PathLike = None,
        slurm_log_filename: PathLike = None,
        slurm_nodes: int = None,
        slurm_partition: str = None,
        modules: [PathLike] = None,
        path_prefix: str = None,
        write_slurm_directory: bool = False,
    ):
        """
        Instantiate a new job script, to run locally or from a job manager.

        :param platform: HPC to run script on
        :param commands: shell commands to run in script
        :param slurm_tasks: number of total tasks for Slurm to run
        :param slurm_account: Slurm account name
        :param slurm_duration: duration to run job in job manager
        :param slurm_run_name: Slurm run name
        :param slurm_email_type: email type
        :param slurm_email_address: email address
        :param slurm_error_filename: file path to error log file
        :param slurm_log_filename: file path to output log file
        :param slurm_nodes: number of physical nodes to run on
        :param slurm_partition: partition to run on (stampede2 only)
        :param modules: file paths to modules to load
        :param path_prefix: file path to prepend to the PATH
        :param write_slurm_directory: explicitly add directory to Slurm header when writing file
        """

        if isinstance(modules, Sequence) and len(modules) == 0:
            modules = None

        if platform == Platform.STAMPEDE2 and slurm_partition is None:
            slurm_partition = 'development'

        self.platform = platform
        self.commands = commands if commands is not None else []

        self.slurm_tasks = slurm_tasks
        self.slurm_account = slurm_account
        self.slurm_duration = slurm_duration

        self.__slurm_run_directory = None
        self.slurm_run_name = (
            slurm_run_name if slurm_run_name is not None else uuid.uuid4().hex
        )
        self.slurm_email_type = slurm_email_type
        self.slurm_email_address = slurm_email_address

        self.slurm_error_filename = (
            slurm_error_filename if slurm_error_filename is not None else 'slurm.log'
        )
        self.slurm_log_filename = (
            slurm_log_filename if slurm_log_filename is not None else 'slurm.log'
        )
        self.slurm_nodes = slurm_nodes

        self.slurm_partition = slurm_partition

        self.modules = modules
        self.path_prefix = path_prefix
        self.write_slurm_directory = write_slurm_directory

    @property
    def launcher(self) -> str:
        """
        :return: command to start processes on target system (`srun`, `ibrun`, etc.)
        """

        if self.platform in [Platform.HERA, Platform.ORION]:
            return 'srun'
        elif self.platform in [Platform.STAMPEDE2]:
            return 'ibrun'
        else:
            return ''

    @property
    def slurm_tasks(self) -> int:
        return self.__slurm_tasks

    @slurm_tasks.setter
    def slurm_tasks(self, slurm_tasks: int = 1):
        self.__slurm_tasks = int(slurm_tasks)

    @property
    def slurm_nodes(self) -> int:
        return self.__slurm_nodes

    @slurm_nodes.setter
    def slurm_nodes(self, slurm_nodes: int):
        if slurm_nodes is None and self.platform == Platform.STAMPEDE2:
            slurm_nodes = numpy.ceil(self.slurm_tasks / 68)
        if slurm_nodes is not None:
            slurm_nodes = int(slurm_nodes)
        self.__slurm_nodes = slurm_nodes

    @property
    def slurm_header(self) -> str:
        lines = [f'#SBATCH -J {self.slurm_run_name}']

        if self.__slurm_run_directory is not None:
            lines.append(f'#SBATCH -D {self.__slurm_run_directory}')

        if self.slurm_account is not None:
            lines.append(f'#SBATCH -A {self.slurm_account}')
        if self.slurm_email_type not in (None, SlurmEmailType.NONE):
            lines.append(f'#SBATCH --mail-type={self.slurm_email_type.value}')
            if self.slurm_email_address is not None and len(self.slurm_email_address) > 0:
                lines.append(f'#SBATCH --mail-user={self.slurm_email_address}')
            else:
                raise ValueError('missing email address')
        if self.slurm_error_filename is not None:
            lines.append(f'#SBATCH --error={self.slurm_error_filename}')
        if self.slurm_log_filename is not None:
            lines.append(f'#SBATCH --output={self.slurm_log_filename}')

        lines.append(f'#SBATCH -n {self.slurm_tasks}')
        if self.slurm_nodes is not None:
            lines.append(f'#SBATCH -N {self.slurm_nodes}')

        hours, remainder = divmod(self.slurm_duration, timedelta(hours=1))
        minutes, remainder = divmod(remainder, timedelta(minutes=1))
        seconds = round(remainder / timedelta(seconds=1))

        lines.append(f'#SBATCH --time={hours:02}:{minutes:02}:{seconds:02}')
        if self.slurm_partition is not None:
            lines.append(f'#SBATCH --partition={self.slurm_partition}')

        return '\n'.join(lines)

    def __str__(self) -> str:
        lines = [
            self.shebang,
        ]

        if self.platform != Platform.LOCAL:
            lines.extend(
                [self.slurm_header, '', 'set -e', '']
            )

        if self.modules is not None:
            modules_string = ' '.join(module for module in self.modules)
            lines.extend([f'module load {modules_string}', ''])

        if self.path_prefix is not None:
            lines.extend([f'PATH={self.path_prefix}:$PATH', ''])

        lines.extend(str(command) for command in self.commands)

        return '\n'.join(lines)

    def write(self, filename: PathLike, overwrite: bool = False):
        """
        Write script to file.

        :param filename: path to output file
        :param overwrite: whether to overwrite existing files
        """

        if not isinstance(filename, Path):
            filename = Path(filename)

        if filename.is_dir():
            filename = filename / f'{self.platform.value}.job'

        if self.write_slurm_directory:
            self.__slurm_run_directory = filename.parent

        output = f'{self}\n'
        if overwrite or not filename.exists():
            with open(filename, 'w') as file:
                file.write(output)

        if self.write_slurm_directory:
            self.__slurm_run_directory = None


class AdcircJobScript(JobScript):
    def __init__(
        self,
        platform: Platform,
        commands: [str],
        slurm_tasks: int,
        slurm_account: str,
        slurm_duration: timedelta,
        slurm_run_name: str,
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

        if self.platform == Platform.STAMPEDE2:
            self.commands.append(
                'source /work/07531/zrb/stampede2/builds/ADC-WW3-NWM-NEMS/modulefiles/envmodules_intel.stampede'
            )
        elif self.platform == Platform.HERA:
            self.commands.append(
                # 'source /scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/modulefiles/envmodules_intel.hera'
                'source /scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/modulefiles/hera/ESMF_NUOPC'
            )


class AdcircRunScript(AdcircJobScript):
    """ script for running ADCIRC via a NEMS configuration """

    def __init__(
        self,
        platform: Platform,
        fort15_filename: PathLike,
        nems_configure_filename: PathLike,
        model_configure_filename: PathLike,
        atm_namelist_rc_filename: PathLike,
        config_rc_filename: PathLike,
        slurm_tasks: int,
        slurm_account: str,
        slurm_duration: timedelta,
        slurm_run_name: str,
        fort67_filename: PathLike = None,
        nems_path: PathLike = None,
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

        self.fort15_filename = PurePosixPath(fort15_filename)
        self.nems_configure_filename = PurePosixPath(nems_configure_filename)
        self.model_configure_filename = PurePosixPath(model_configure_filename)
        self.atm_namelist_rc_filename = PurePosixPath(atm_namelist_rc_filename)
        self.config_rc_filename = PurePosixPath(config_rc_filename)
        self.fort67_filename = PurePosixPath(fort67_filename) if fort67_filename is not None else None

        if nems_path is None:
            if self.platform == Platform.HERA:
                nems_path = '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/NEMS/exe/NEMS.x'
            else:
                nems_path = 'NEMS.x'
        self.nems_path = nems_path

        self.commands.extend(
            [
                '',
                f'ln -sf {self.fort15_filename} ./fort.15',
                f'ln -sf {self.nems_configure_filename} ./nems.configure',
                f'ln -sf {self.model_configure_filename} ./model_configure',
                f'ln -sf {self.atm_namelist_rc_filename} ./atm_namelist.rc',
                f'ln -sf {self.config_rc_filename} ./config.rc',
                '',
            ]
        )

        if self.fort67_filename is not None:
            self.commands.extend(
                [
                    f'ln -sf {self.fort67_filename} ./fort.67.nc',
                    '',
                ]
            )

        self.commands.append(f'{self.launcher} {self.nems_path}')


class AdcircMeshPartitionScript(AdcircJobScript):
    """ script for performing domain decomposition with `adcprep` """

    def __init__(
        self,
        platform: Platform,
        adcirc_mesh_partitions: int,
        slurm_account: str,
        slurm_duration: timedelta,
        slurm_run_name: str,
        adcprep_path: PathLike = None,
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
            if self.platform == Platform.HERA:
                adcprep_path = '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/ADCIRC/work/adcprep'
            else:
                adcprep_path = 'adcprep'
        self.adcprep_path = adcprep_path

        self.commands.extend(
            [
                '',
                f'{self.launcher} {self.adcprep_path} --np {self.adcirc_partitions} --partmesh',
                f'{self.launcher} {self.adcprep_path} --np {self.adcirc_partitions} --prepall',
            ]
        )


class RunScript(Script):
    def __init__(self, platform: Platform):
        self.platform = platform

    def __str__(self) -> str:
        lines = [
            'DIRECTORY="$(',
            '    cd "$(dirname "$0")" >/dev/null 2>&1',
            '    pwd -P',
            ')"',
            '',
            '# prepare single coldstart directory',
            f'cd $DIRECTORY/coldstart',
            f'ln -sf ../job_adcprep_{self.platform.value}.job adcprep.job',
            f'ln -sf ../job_nems_adcirc_{self.platform.value}.job.coldstart nems_adcirc.job',
            'cd $DIRECTORY',
            '',
            '# prepare every hotstart directory',
            bash_for_loop(
                'for hotstart in $DIRECTORY//runs/*/',
                [
                    'cd "$hotstart"',
                    f'ln -sf ../../job_adcprep_{self.platform.value}.job adcprep.job',
                    f'ln -sf ../../job_nems_adcirc_{self.platform.value}.job.hotstart nems_adcirc.job',
                    'cd $DIRECTORY/',
                ]
            ),
            '',
            '# run single coldstart configuration',
            'cd $DIRECTORY/coldstart',
            self.coldstart,
            'cd $DIRECTORY',
            '',
            '# run every hotstart configuration',
            bash_for_loop(
                'for hotstart in $DIRECTORY/runs/*/',
                [
                    'cd "$hotstart"',
                    self.hotstart,
                    'cd $DIRECTORY',
                ]
            ),
        ]

        if self.platform != Platform.LOCAL:
            # slurm queue output https://slurm.schedmd.com/squeue.html
            squeue_command = 'squeue -u $USER -o "%.8i %.21j %.4C %.4D %.31E %.7a %.9P %.20V %.20S %.20e"'
            echo_squeue_command = squeue_command.replace('"', r'\"')
            lines.extend([
                '',
                '# display job queue with dependencies',
                f'echo {echo_squeue_command}',
                squeue_command,
            ])

        return '\n'.join(lines)

    @property
    def coldstart(self) -> str:
        lines = []
        if self.platform != Platform.LOCAL:
            lines.extend(
                [
                    "coldstart_adcprep_jobid=$(sbatch adcprep.job | awk '{print $NF}')",
                    "coldstart_jobid=$(sbatch --dependency=afterany:$coldstart_adcprep_jobid nems_adcirc.job | awk '{print $NF}')",
                ]
            )
        else:
            lines.extend(
                [
                    'sh adcprep.job',
                    'sh nems_adcirc.job',
                ]
            )
        return '\n'.join(lines)

    @property
    def hotstart(self) -> str:
        lines = []
        if self.platform != Platform.LOCAL:
            lines.extend(
                [
                    "hotstart_adcprep_jobid=$(sbatch --dependency=afterany:$coldstart_jobid adcprep.job | awk '{print $NF}')",
                    'sbatch --dependency=afterany:$hotstart_adcprep_jobid nems_adcirc.job',
                ]
            )
        else:
            lines.extend(
                [
                    'sh adcprep.job',
                    'sh nems_adcirc.job',
                ]
            )
        return '\n'.join(lines)

    def write(self, filename: PathLike, overwrite: bool = False):
        if not isinstance(filename, Path):
            filename = Path(filename)

        if filename.is_dir():
            filename = filename / f'run_{self.platform.value}.sh'

        output = f'{self}\n'
        if overwrite or not filename.exists():
            with open(filename, 'w') as file:
                file.write(output)


def bash_if_statement(
    condition: str, then: [str], *else_then: [[str]], indentation: str = '    '
) -> str:
    """
    Create a if statement in Bash syntax using the given condition, then statement(s), and else condition(s) / statement(s).

    :param condition: boolean condition to check
    :param then: Bash statement(s) to execute if condition is met
    :param else_then: arbitrary number of Bash statement(s) to execute if condition is not met, with optional conditions (`elif`)
    :param indentation: indentation
    :return: if statement
    """

    if not isinstance(then, str) and isinstance(then, Sequence):
        then = '\n'.join(then)

    condition = str(condition).strip('if ').strip('; then')

    lines = [f'if {condition}; then', textwrap.indent(then, indentation)]

    for else_block in else_then:
        if not isinstance(else_block, str) and isinstance(else_block, Sequence):
            else_block = '\n'.join(else_block)

        currently_else = else_block.startswith('else')
        currently_elif = else_block.startswith('elif')
        currently_then = else_block.startswith('then')

        previous_line = lines[-1].strip()
        hanging_else = previous_line.startswith('else') and not previous_line.endswith(';')
        hanging_elif = previous_line.startswith('elif') and not previous_line.endswith(';')

        if currently_else or currently_elif:
            if hanging_else or hanging_elif:
                lines.remove(-1)
            if currently_else:
                lines.append(else_block)
            elif currently_elif:
                else_block.strip('elif ')
                lines.append(f'elif {else_block}')
        else:
            if not hanging_else and not hanging_elif:
                lines.append('else')
            elif hanging_elif:
                lines[-1].append(';')
                if not currently_then:
                    lines[-1].append(' then')
            lines.append(textwrap.indent(else_block, indentation))

    lines.append('fi')

    return '\n'.join(lines)


def bash_for_loop(iteration: str, do: [str], indentation='    ') -> str:
    """
    Create a for loop in Bash syntax using the given variable, iterator, and do statement(s).

    :param iteration: for loop statement, such as `for dir in ./*`
    :param do: Bash statement(s) to execute on every loop iteration
    :param indentation: indentation
    :return: for loop
    """

    if not isinstance(do, str) and isinstance(do, Sequence):
        do = '\n'.join(do)

    return '\n'.join((f'{iteration}; do', textwrap.indent(do, indentation), 'done',))


def bash_function(name: str, body: [str], indentation: str = '    ') -> str:
    """
    Create a function in Bash syntax using the given name and function statement(s).

    :param name: name of function
    :param body: Bash statement(s) making up function body
    :param indentation: indentation
    :return: function
    """

    if not isinstance(body, str) and isinstance(body, Sequence):
        body = '\n'.join(body)

    return '\n'.join([f'{name}() {{', textwrap.indent(body, indentation), '}'])
