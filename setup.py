import sys

import gartersnake
from setuptools import config, find_packages, setup

DEPENDENCIES = {
    'adcircpy>=1.1.2': ['gdal', 'fiona'],
    'file-read-backwards': [],
    'nemspy>=1.0.4': [],
    'numpy': [],
    'pyproj': [],
    'typepigeon>=1.0.3': [],
}

MISSING_DEPENDENCIES = gartersnake.missing_requirements(DEPENDENCIES)

if len(MISSING_DEPENDENCIES) > 0:
    print(f'{len(MISSING_DEPENDENCIES)} (out of {len(DEPENDENCIES)}) dependencies are missing')

if len(MISSING_DEPENDENCIES) > 0 and gartersnake.is_conda():
    print(f'found conda environment at {sys.prefix}')
    gartersnake.install_conda_requirements(MISSING_DEPENDENCIES)
    MISSING_DEPENDENCIES = gartersnake.missing_requirements(DEPENDENCIES)

if len(MISSING_DEPENDENCIES) > 0 and gartersnake.is_windows():
    gartersnake.install_windows_requirements(MISSING_DEPENDENCIES)
    MISSING_DEPENDENCIES = gartersnake.missing_requirements(DEPENDENCIES)

__version__ = gartersnake.vcs_version()
print(f'using version {__version__}')

metadata = config.read_configuration('setup.cfg')['metadata']

setup(
    **metadata,
    version=__version__,
    long_description_content_type='text/markdown',
    packages=find_packages(),
    python_requires='>=3.7',
    setup_requires=['dunamai', 'gartersnake', 'setuptools>=41.2'],
    install_requires=list(DEPENDENCIES),
    extras_require={
        'testing': ['filelock', 'pytest', 'pytest-cov', 'pytest-xdist', 'wget'],
        'development': ['flake8', 'isort', 'oitnb'],
        'documentation': [
            'dunamai',
            'm2r2',
            'sphinx',
            'sphinx-rtd-theme',
            'sphinxcontrib-programoutput',
        ],
    },
    entry_points={
        'console_scripts': [
            'initialize_adcirc=coupledmodeldriver.client.initialize_adcirc:main',
            'generate_adcirc=coupledmodeldriver.client.generate_adcirc:main',
            'check_completion=coupledmodeldriver.client.check_completion:main',
            'unqueued_runs=coupledmodeldriver.client.unqueued_runs:main',
        ],
    },
)
