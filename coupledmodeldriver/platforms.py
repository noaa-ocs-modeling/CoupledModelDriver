from enum import Enum


class Platform(Enum):
    HERA = \
        {
            'source_filename': '/scratch2/COASTAL/coastal/save/shared/repositories/ADC-WW3-NWM-NEMS/modulefiles/envmodules_intel.hera',
            'virtual_nodes': True,
            'processors_per_node': 40,
            'launcher': 'srun',
            'slurm': True,
            'account': 'coastal',
            'partition': 'coastal',
        }
    STAMPEDE2 = \
        {
            'source_filename': '/work/07531/zrb/stampede2/builds/ADC-WW3-NWM-NEMS/modulefiles/envmodules_intel.stampede',
            'virtual_nodes': True,
            'processors_per_node': 68,
            'launcher': 'ibrun',
            'slurm': True,
            'account': 'coastal',
            'partition': 'development',
        }
    LOCAL = \
        {
            'source_filename': None,
            'virtual_nodes': False,
            'processors_per_node': 1,
            'launcher': '',
            'slurm': False,
            'account': None,
            'partition': None,
        }
