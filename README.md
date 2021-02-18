# CoupledModelDriver

[![tests](https://github.com/noaa-ocs-modeling/CoupledModelDriver/workflows/tests/badge.svg)](https://github.com/noaa-ocs-modeling/CoupledModelDriver/actions?query=workflow%3Atests)
[![build](https://github.com/noaa-ocs-modeling/CoupledModelDriver/workflows/build/badge.svg)](https://github.com/noaa-ocs-modeling/CoupledModelDriver/actions?query=workflow%3Abuild)
[![version](https://img.shields.io/pypi/v/CoupledModelDriver)](https://pypi.org/project/CoupledModelDriver)
[![license](https://img.shields.io/github/license/noaa-ocs-modeling/CoupledModelDriver)](https://creativecommons.org/share-your-work/public-domain/cc0)
[![style](https://sourceforge.net/p/oitnb/code/ci/default/tree/_doc/_static/oitnb.svg?format=raw)](https://sourceforge.net/p/oitnb/code)

`coupledmodeldriver` generates an overlying job submission framework and configuration directories for NEMS-coupled coastal
ocean model ensembles.

It utilizes [`nemspy`](https://github.com/noaa-ocs-modeling/NEMSpy) to generate NEMS configuration files, shares common
configurations between runs, and organizes spinup and mesh partition into separate jobs for dependant submission.

### Supported models and inputs:

- circulation models
    - ADCIRC (uses [`adcircpy`](https://github.com/JaimeCalzadaNOAA/adcircpy))
- forcing
    - ATMESH
    - WW3DATA

### Supported platforms:

- local (no job manager)
- Hera (Slurm)
- Stampede2 (Slurm)
