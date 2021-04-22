from datetime import timedelta
from enum import Enum
from os import PathLike
from pathlib import Path
import textwrap
from typing import Sequence
import uuid

import numpy

from coupledmodeldriver.platforms import Platform
from coupledmodeldriver.utilities import make_executable


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


class Script:
    shebang = '#!/bin/bash --login'

    def __init__(self, commands: [str]):
        if commands is None:
            commands = []
        elif isinstance(commands, str):
            commands = [commands]
        self.commands = commands

    def __str__(self) -> str:
        return '\n'.join([self.shebang, *(str(command) for command in self.commands)])

    def write(self, filename: PathLike, overwrite: bool = False):
        """
        Write script to file.

        :param filename: path to output file
        :param overwrite: whether to overwrite existing files
        """

        if not isinstance(filename, Path):
            filename = Path(filename)

        output = f'{self}\n'
        if overwrite or not filename.exists():
            with open(filename, 'w') as file:
                file.write(output)
        else:
            raise FileExistsError(f'file exists at {filename}')


class JobScript(Script):
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

        super().__init__(commands)

        if isinstance(modules, Sequence) and len(modules) == 0:
            modules = None

        if slurm_partition is None:
            slurm_partition = platform.value['default_partition']

        self.platform = platform

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

        return self.platform.value['launcher']

    @property
    def slurm_tasks(self) -> int:
        return self.__slurm_tasks

    @slurm_tasks.setter
    def slurm_tasks(self, slurm_tasks: int = 1):
        if slurm_tasks is not None:
            slurm_tasks = int(slurm_tasks)
        self.__slurm_tasks = slurm_tasks

    @property
    def slurm_nodes(self) -> int:
        return self.__slurm_nodes

    @slurm_nodes.setter
    def slurm_nodes(self, slurm_nodes: int):
        if slurm_nodes is None:
            slurm_nodes = numpy.ceil(
                self.slurm_tasks / self.platform.value['processors_per_node']
            )
        if slurm_nodes is not None:
            slurm_nodes = int(slurm_nodes)
        self.__slurm_nodes = slurm_nodes

    @property
    def slurm_header(self) -> str:
        lines = []

        if self.slurm_run_name is not None:
            lines.append(f'#SBATCH -J {self.slurm_run_name}')

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

        if self.slurm_tasks is not None:
            lines.append(f'#SBATCH -n {self.slurm_tasks}')
        if self.slurm_nodes is not None:
            lines.append(f'#SBATCH -N {self.slurm_nodes}')

        if self.slurm_duration is not None:
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

        if self.platform.value['uses_slurm']:
            lines.extend([self.slurm_header, '', 'set -e', ''])

        if self.modules is not None:
            modules_string = ' '.join(module for module in self.modules)
            lines.extend([f'module load {modules_string}', ''])

        if self.path_prefix is not None:
            lines.extend([f'PATH={self.path_prefix}:$PATH', ''])

        lines.extend(str(command) for command in self.commands)

        return '\n'.join(lines)

    def write(self, filename: PathLike, overwrite: bool = False):
        if not isinstance(filename, Path):
            filename = Path(filename)

        if filename.is_dir():
            filename = filename / f'{self.platform.name.lower()}.job'

        if self.write_slurm_directory:
            self.__slurm_run_directory = filename.parent

        super().write(filename, overwrite)

        if self.write_slurm_directory:
            self.__slurm_run_directory = None


class EnsembleRunScript(Script):
    def __init__(self, platform: Platform, commands: [str] = None):
        self.platform = platform
        super().__init__(commands)

    def __str__(self) -> str:
        lines = [
            'DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"',
            '',
            '# run single coldstart configuration',
            'pushd ${DIRECTORY}/coldstart >/dev/null 2>&1',
            self.coldstart,
            'popd >/dev/null 2>&1',
            '',
            '# run every hotstart configuration',
            bash_for_loop(
                'for hotstart in ${DIRECTORY}/runs/*/',
                ['pushd ${hotstart} >/dev/null 2>&1', self.hotstart, 'popd >/dev/null 2>&1'],
            ),
            *(str(command) for command in self.commands),
        ]

        if self.platform.value['uses_slurm']:
            # slurm queue output https://slurm.schedmd.com/squeue.html
            squeue_command = 'squeue -u $USER -o "%.8i %3C %4D %97Z %15j" --sort i'
            echo_squeue_command = squeue_command.replace('"', r'\"')
            lines.extend(
                [
                    '',
                    '# display job queue with dependencies',
                    squeue_command,
                    f'echo {echo_squeue_command}',
                ]
            )

        return '\n'.join(lines)

    @property
    def coldstart(self) -> str:
        lines = []
        if self.platform.value['uses_slurm']:
            lines.extend(
                [
                    "coldstart_adcprep_jobid=$(sbatch adcprep.job | awk '{print $NF}')",
                    "coldstart_jobid=$(sbatch --dependency=afterany:$coldstart_adcprep_jobid adcirc.job | awk '{print $NF}')",
                ]
            )
        else:
            lines.extend(['sh adcprep.job', 'sh adcirc.job'])
        return '\n'.join(lines)

    @property
    def hotstart(self) -> str:
        lines = []
        if self.platform.value['uses_slurm']:
            lines.extend(
                [
                    "hotstart_adcprep_jobid=$(sbatch --dependency=afterany:$coldstart_jobid adcprep.job | awk '{print $NF}')",
                    'sbatch --dependency=afterany:$hotstart_adcprep_jobid adcirc.job',
                ]
            )
        else:
            lines.extend(['sh adcprep.job', 'sh adcirc.job'])
        return '\n'.join(lines)

    def write(self, filename: PathLike, overwrite: bool = False):
        if not isinstance(filename, Path):
            filename = Path(filename)

        if filename.is_dir():
            filename = filename / f'run_{self.platform.name.lower()}.sh'

        super().write(filename, overwrite)

        if not filename.exists() or overwrite:
            make_executable(filename)


class EnsembleCleanupScript(Script):
    """ script for cleaning up ADCIRC NEMS configurations """

    def __init__(self, commands: [str] = None):
        super().__init__(commands)

    def __str__(self):
        lines = [
            'DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"',
            '',
            '# prepare single coldstart directory',
            'pushd ${DIRECTORY}/coldstart >/dev/null 2>&1',
            'rm -rf PE* ADC_*',
            'rm max* partmesh.txt metis_graph.txt',
            'rm fort.16 fort.6* fort.80',
            'popd >/dev/null 2>&1',
            '',
            '# prepare every hotstart directory',
            bash_for_loop(
                'for hotstart in ${DIRECTORY}/runs/*/',
                [
                    'pushd ${hotstart} >/dev/null 2>&1',
                    'rm -rf PE* ADC_*',
                    'rm max* partmesh.txt metis_graph.txt',
                    'rm fort.16 fort.6* fort.80',
                    'popd >/dev/null 2>&1',
                ],
            ),
            *(str(command) for command in self.commands),
        ]

        return '\n'.join(lines)

    def write(self, filename: PathLike, overwrite: bool = False):
        if not isinstance(filename, Path):
            filename = Path(filename)

        if filename.is_dir():
            filename = filename / f'cleanup.sh'

        super().write(filename, overwrite)


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

    return '\n'.join((f'{iteration}; do', textwrap.indent(do, indentation), 'done'))


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
