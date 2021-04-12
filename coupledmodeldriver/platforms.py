from enum import Enum


class Platform(Enum):
    HERA = {
        'source_filename': '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/modulefiles/envmodules_intel.hera',
        'processors_per_node': 40,
        'launcher': 'srun',
        'uses_slurm': True,
        'slurm_account': 'coastal',
        'default_partition': None,
    }
    STAMPEDE2 = {
        'source_filename': '/work/07531/zrb/stampede2/builds/ADC-WW3-NWM-NEMS/modulefiles/envmodules_intel.stampede',
        'processors_per_node': 68,
        'launcher': 'ibrun',
        'uses_slurm': True,
        'slurm_account': 'coastal',
        'default_partition': 'development',
    }
    LOCAL = {
        'source_filename': None,
        'processors_per_node': 1,
        'launcher': None,
        'uses_slurm': False,
        'slurm_account': None,
        'default_partition': None,
    }
