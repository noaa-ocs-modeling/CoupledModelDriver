import logging

from coupledmodeldriver._depend import optional_import

logging.basicConfig(format='[%(asctime)s] %(name)-9s %(levelname)-8s: %(message)s')

__all__ = [
    'check_completion',
    'generate_schism',
    'initialize_schism',
    'unqueued_runs',
]

if optional_import('adcircpy') is not None:
    __all__.extend(
        ['generate_adcirc', 'initialize_adcirc',]
    )
