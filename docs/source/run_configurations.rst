Run Configurations
==================

The :code:`RunConfiguration` is the underlying class used by :code:`initialize_adcirc` command, and stores individual JSON configurations (`configure_adcirc.json`, `configure_nems.json`, etc.) as Python :code:`ConfigurationJSON` objects.

The following Python script generates a set of JSON configuration files, encapsulating all values for a NEMS+ADCIRC+tidal+WW3DATA run, to be submitted via Slurm to Hera.

.. code-block:: python

    from datetime import datetime, timedelta

    from adcircpy.forcing.tides import Tides
    from adcircpy.forcing.tides.tides import TidalSource
    from adcircpy.forcing.waves.ww3 import WaveWatch3DataForcing
    from adcircpy.forcing.winds.atmesh import AtmosphericMeshForcing

    from coupledmodeldriver import Platform
    from coupledmodeldriver.generate import NEMSADCIRCRunConfiguration

    # initialize `adcircpy` forcing objects
    tidal_forcing = Tides(
        tidal_source=TidalSource.TPXO,
        resource='/scratch2/COASTAL/coastal/save/shared/models/forcings/tides/h_tpxo9.v1.nc',
    )
    tidal_forcing.use_all()
    wind_forcing = AtmosphericMeshForcing(
        filename='/scratch2/COASTAL/coastal/save/shared/models/forcings/shinnecock/ike/wind_atm_fin_ch_time_vec.nc',
        nws=17,
        interval_seconds=3600,
    )
    wave_forcing = WaveWatch3DataForcing(
        filename='/scratch2/COASTAL/coastal/save/shared/models/forcings/shinnecock/ike/ww3.Constant.20151214_sxy_ike_date.nc',
        nrs=5,
        interval_seconds=3600,
    )
    forcings = [tidal_forcing, wind_forcing, wave_forcing]

    # initialize configuration object
    configuration = NEMSADCIRCRunConfiguration(
        mesh_directory='/scratch2/COASTAL/coastal/save/shared/models/meshes/shinnecock/v1.0',
        modeled_start_time=datetime(year=2008, month=8, day=23),
        modeled_end_time=datetime(year=2008, month=8, day=23) + timedelta(days=14.5),
        modeled_timestep=timedelta(seconds=2),
        nems_interval=timedelta(hours=1),
        nems_connections=['ATM -> OCN', 'WAV -> OCN'],
        nems_mediations=None,
        nems_sequence=['ATM -> OCN', 'WAV -> OCN', 'ATM', 'WAV', 'OCN'],
        tidal_spinup_duration=timedelta(days=12.5),
        platform=Platform.HERA,
        perturbations=None,
        forcings=forcings,
        adcirc_processors=256,
        slurm_partition=None,
        slurm_job_duration=timedelta(hours=6),
        slurm_email_address=None,
        nems_executable='/scratch2/COASTAL/coastal/save/shared/repositories/CoastalApp/ALLBIN_INSTALL/NEMS-adcirc-atmesh-ww3data.x',
        adcprep_executable='/scratch2/COASTAL/coastal/save/shared/repositories/CoastalApp/ALLBIN_INSTALL/adcprep',
        source_filename='/scratch2/COASTAL/coastal/save/shared/repositories/CoastalApp/modulefiles/envmodules_intel.hera',
    )

    # write configuration to `*.json` files
    configuration.write_directory('hera_shinnecock_ike_spinup_tidal_atmesh_ww3data')

ADCIRC-only run configuration
-----------------------------
.. autoclass:: coupledmodeldriver.generate.ADCIRCRunConfiguration

NEMS-ADCIRC run configuration
-----------------------------
.. autoclass:: coupledmodeldriver.generate.NEMSADCIRCRunConfiguration
