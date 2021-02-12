#!/usr/bin/env python
import logging
import os

from setuptools import config, find_packages, setup

if os.name == 'nt':
    import subprocess
    import sys

    try:
        import pipwin
    except ImportError:
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'pipwin'])

    try:
        import gdal
    except ImportError:
        subprocess.check_call([sys.executable, '-m', 'pipwin', 'install', 'gdal'])

    try:
        import fiona
    except ImportError:
        subprocess.check_call([sys.executable, '-m', 'pipwin', 'install', 'fiona'])

try:
    try:
        from dunamai import Version
    except ImportError:
        import subprocess
        import sys

        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'dunamai'])
        from dunamai import Version

    version = Version.from_any_vcs().serialize()
except RuntimeError as error:
    logging.exception(error)
    version = '0.0.0'

logging.info(f'using version {version}')

metadata = config.read_configuration('setup.cfg')['metadata']

setup(
    name=metadata['name'],
    version=version,
    author=metadata['author'],
    author_email=metadata['author_email'],
    description=metadata['description'],
    long_description=metadata['long_description'],
    long_description_content_type='text/markdown',
    url=metadata['url'],
    packages=find_packages(),
    python_requires='>=3.6',
    setup_requires=['dunamai', 'setuptools>=41.2'],
    install_requires=['adcircpy==1.0.17', 'nemspy>=0.6.2', 'numpy', 'requests'],
    extras_require={'testing': ['flake8', 'pytest', 'pytest-cov'], 'development': ['oitnb']},
)
