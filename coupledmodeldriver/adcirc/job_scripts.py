from datetime import timedelta
from os import PathLike
from pathlib import Path

from .. import Platform
from ..job_scripts import JobScript, Script


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

        if source_filename is not None and len(str(source_filename)) > 0:
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
        self.executable = executable

        self.commands.append(f'{self.launcher} {self.executable}')


class AdcircMeshPartitionJob(AdcircJob):
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
            adcprep_path = 'adcprep'
        self.adcprep_path = adcprep_path

        self.commands.extend(
            [
                f'{self.launcher} {self.adcprep_path} --np {self.adcirc_partitions} --partmesh',
                f'{self.launcher} {self.adcprep_path} --np {self.adcirc_partitions} --prepall',
            ]
        )


class ADCIRCGenerationScript(Script):
    """ Python script for generating ADCIRC NEMS configurations """

    def __init__(self, commands: [str] = None):
        super().__init__(commands)

    def __str__(self):
        lines = [
            'from pathlib import Path',
            '',
            'from coupledmodeldriver.adcirc.adcirc import generate_adcirc_configuration',
            '',
            '',
            "if __name__ == '__main__':",
            '    generate_adcirc_configuration(output_directory=Path(__file__).parent, overwrite=True)',
        ]

        return '\n'.join(lines)

    def write(self, filename: PathLike, overwrite: bool = False):
        if not isinstance(filename, Path):
            filename = Path(filename)

        if filename.is_dir():
            filename = filename / f'generate_adcirc.py'

        super().write(filename, overwrite)
