from abc import ABC, abstractmethod
from datetime import timedelta
from enum import Enum
from os import PathLike
from pathlib import Path
import textwrap
from typing import List, Sequence, Iterable
import uuid

import numpy

from coupledmodeldriver.platforms import Platform
from coupledmodeldriver.utilities import LOGGER, make_executable


class SlurmEmailType(Enum):
    """
    option for Slurm email notification
    """

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
    """
    abstraction of an executable script
    """

    shebang = '#!/bin/bash'

    def __init__(self, commands: List[str]):
        if commands is None:
            commands = []
        elif isinstance(commands, str):
            commands = [commands]
        self.commands = commands

    def __str__(self) -> str:
        lines = []

        if self.shebang is not None:
            lines.append(self.shebang)

        lines.extend(self.commands)

        return '\n'.join(lines)

    def write(self, filename: PathLike, overwrite: bool = False):
        """
        write script to file

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
            LOGGER.warning(f'skipping existing file "{filename}"')


class JobScript(Script):
    """
    abstraction of a Slurm job script, to run locally or from a job manager
    """

    def __init__(
        self,
        platform: Platform,
        commands: List[str],
        slurm_run_name: str = None,
        slurm_tasks: int = None,
        slurm_duration: timedelta = None,
        slurm_account: str = None,
        slurm_email_type: SlurmEmailType = None,
        slurm_email_address: str = None,
        slurm_error_filename: PathLike = None,
        slurm_log_filename: PathLike = None,
        slurm_nodes: int = None,
        slurm_partition: str = None,
        modules: List[PathLike] = None,
        path_prefix: str = None,
        write_slurm_directory: bool = False,
    ):
        """
        :param platform: HPC to run script on
        :param commands: shell commands to run in script
        :param slurm_run_name: Slurm run name
        :param slurm_tasks: number of total tasks for Slurm to run
        :param slurm_duration: duration to run job in job manager
        :param slurm_account: Slurm account name
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

        if slurm_run_name is None:
            slurm_run_name = uuid.uuid4().hex

        if slurm_tasks is None:
            slurm_tasks = platform.value['processors_per_node']

        if slurm_duration is None:
            slurm_duration = timedelta(hours=1)

        if slurm_account is None:
            slurm_account = platform.value['slurm_account']

        if slurm_partition is None:
            slurm_partition = platform.value['default_partition']

        self.platform = platform

        self.slurm_tasks = slurm_tasks
        self.slurm_account = slurm_account
        self.slurm_duration = slurm_duration

        self.__slurm_run_directory = None
        self.slurm_run_name = slurm_run_name
        self.slurm_email_type = slurm_email_type
        self.slurm_email_address = slurm_email_address

        self.slurm_error_filename = slurm_error_filename
        self.slurm_log_filename = (
            slurm_log_filename if slurm_log_filename is not None else f'{slurm_run_name}.log'
        )
        self.slurm_nodes = slurm_nodes

        self.slurm_partition = slurm_partition

        self.modules = modules
        self.path_prefix = path_prefix
        self.write_slurm_directory = write_slurm_directory

    @property
    def launcher(self) -> str:
        """
        command to start processes on target system (``srun``, ``ibrun``, etc.)
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
        lines = []

        if self.shebang is not None:
            lines.append(self.shebang)

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


class EnsembleGenerationJob(JobScript):
    """
    job script to generate the ensemble configuration
    """

    def __init__(
        self,
        platform: Platform,
        generate_command: str,
        slurm_tasks: int = None,
        slurm_duration: timedelta = None,
        slurm_account: str = None,
        slurm_run_name: str = 'GENERATE_CONFIGURATION',
        commands: List[str] = None,
        parallel: bool = False,
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

        if parallel:
            generate_command = f'{generate_command} --parallel'

        self.commands.extend(
            [generate_command, 'echo "use ./run_<platform>.sh to start model"']
        )


class EnsembleRunScript(Script, ABC):
    """
    script to run the ensemble, either by running it directly or by submitting model execution to the job manager
    default filename is ``run_<platform>.sh``
    """

    def __init__(
        self, platform: Platform, run_spinup: bool = True, commands: List[str] = None,
    ):
        self.platform = platform
        self.run_spinup = run_spinup
        super().__init__(commands)

    @abstractmethod
    def _spinup_lines(self):
        pass

    @abstractmethod
    def _hotstart_lines(self):
        pass

    def __str__(self) -> str:
        lines = []

        if self.shebang is not None:
            lines.append(self.shebang)

        lines.extend(
            [
                *(str(command) for command in self.commands),
                'DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"',
                '',
            ]
        )

        if self.run_spinup:
            lines.extend(self._spinup_lines())

        hotstart_lines = [
            '# run configurations',
            bash_for_loop('for hotstart in ${DIRECTORY}/runs/*/', self._hotstart_lines()),
        ]
        lines.extend(hotstart_lines)

        if self.platform.value['uses_slurm']:
            # slurm queue output https://slurm.schedmd.com/squeue.html
            squeue_command = 'squeue -u $USER -o "%.8i %4C %4D %16E %12R %8M %j" --sort i'
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

    def write(self, filename: PathLike, overwrite: bool = False):
        if not isinstance(filename, Path):
            filename = Path(filename)

        if filename.is_dir():
            filename = filename / f'run_{self.platform.name.lower()}.sh'

        super().write(filename, overwrite)

        if not filename.exists() or overwrite:
            make_executable(filename)


class EnsembleCleanupScript(Script):
    """
    script for cleaning an ensemble configuration, by deleting output and log files
    """

    def __init__(
        self,
        commands: List[str] = None,
        filenames: List[PathLike] = None,
        spinup_filenames: List[PathLike] = None,
        hotstart_filenames: List[PathLike] = None,
    ):

        self._filenames = filenames if isinstance(filenames, Iterable) else []
        self._spinup_filenames = (
            spinup_filenames if isinstance(spinup_filenames, Iterable) else []
        )
        self._hotstart_filenames = (
            hotstart_filenames if isinstance(hotstart_filenames, Iterable) else []
        )
        super().__init__(commands)

    def __str__(self):
        lines = []

        if self.shebang is not None:
            lines.append(self.shebang)

        spinup_filenames = self._filenames + self._spinup_filenames
        hotstart_filenames = self._filenames + self._hotstart_filenames

        lines.extend(
            [
                *(str(command) for command in self.commands),
                'DIRECTORY="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"',
                '',
                '# clean spinup files',
                'rm -rf '
                + ' '.join(
                    f'${{DIRECTORY}}/spinup/{filename}' for filename in spinup_filenames
                ),
                '',
                '# clean run configurations',
                'rm -rf '
                + ' '.join(
                    f'${{DIRECTORY}}/runs/*/{filename}' for filename in hotstart_filenames
                ),
            ]
        )

        return '\n'.join(lines)

    def write(self, filename: PathLike, overwrite: bool = False):
        if not isinstance(filename, Path):
            filename = Path(filename)

        if filename.is_dir():
            filename = filename / f'cleanup.sh'

        super().write(filename, overwrite)


def bash_if_statement(
    condition: str, then: List[str], *else_then: List[List[str]], indentation: str = '    '
) -> str:
    """
    create a if statement in Bash syntax using the given condition, then statement(s), and else condition(s) / statement(s)

    :param condition: boolean condition to check
    :param then: Bash statement(s) to execute if condition is met
    :param else_then: arbitrary number of Bash statement(s) to execute if condition is not met, with optional conditions (``elif``)
    :param indentation: indentation
    :return: if statement as a string
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


def bash_for_loop(iteration: str, do: List[str], indentation='    ') -> str:
    """
    create a for loop in Bash syntax using the given variable, iterator, and do statement(s)

    :param iteration: for loop statement, such as ``for dir in ./*``
    :param do: Bash statement(s) to execute on every loop iteration
    :param indentation: indentation
    :return: for loop as a string
    """

    if not isinstance(do, str) and isinstance(do, Sequence):
        do = '\n'.join(do)

    return '\n'.join((f'{iteration}; do', textwrap.indent(do, indentation), 'done'))


def bash_function(name: str, body: List[str], indentation: str = '    ') -> str:
    """
    create a function in Bash syntax using the given name and function statement(s)

    :param name: name of function
    :param body: Bash statement(s) making up function body
    :param indentation: indentation
    :return: function as a string
    """

    if not isinstance(body, str) and isinstance(body, Sequence):
        body = '\n'.join(body)

    return '\n'.join([f'{name}() {{', textwrap.indent(body, indentation), '}'])


def bash_if(condition, command, oneline=True):

    div = ';' if oneline else '\n'
    return f'if {condition}{div}then {command}{div}fi'


def slurm_dependencies(after_ok: List[str]):
    """
    create dependency argument for sbatch cli based on input list

    :param after_ok: list of dependencies as they should appear on bash script sbatch call
    :return: either an empty string or a dependencies argument for sbatch
    """

    dependency_list = []
    if len(after_ok) > 0:
        dependency_list.append(f'--dependency=afterok:{":".join(after_ok)}')

    return ' '.join(dependency_list)


def slurm_submit_get_id(job_file: PathLike, job_id_var: str, dependencies: str = ''):
    """
    create a script to call a job via sbatch and return the job id as a named variable in bash

    :param job_file: path to the slurm script file
    :param job_id_var: bash variable name to store the submitted slurm job id
    :param dependecies: dependency argument for sbatch command
    :return: bash script to call sbatch with optional dependencies and store job id in the specified bash variable
    """

    # NOTE: `sbatch` will only use `--dependency` if it is BEFORE the job filename
    return f"{job_id_var}=$(sbatch {dependencies} {str(job_file)} | awk '{{print $NF}}')"
