#!/usr/bin/env python
import logging

from setuptools import config, find_packages, setup

try:
    from dunamai import Version
except ImportError:
    import sys
    import subprocess

    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'dunamai'])
    from dunamai import Version

try:
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
    install_requires=['adcircpy', 'nemspy', 'numpy', 'requests'],
    extras_require={
        'testing': ['flake8', 'pytest', 'pytest-cov'],
        'development': ['oitnb']
    },
)
