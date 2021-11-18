CLI Commands
============

``initialize_adcirc`` - create JSON configuration files
-------------------------------------------------------
.. code-block:: shell

    initialize_adcirc \
        --platform HERA \
        --mesh-directory /scratch2/COASTAL/coastal/save/shared/models/meshes/shinnecock/v1.0 \
        --output-directory hera_shinnecock_ike_spinup_tidal_atmesh_ww3data \
        --modeled-start-time 20080823 \
        --modeled-duration 14:06:00:00 \
        --modeled-timestep 00:00:02 \
        --nems-interval 01:00:00 \
        --adcirc-executable /scratch2/COASTAL/coastal/save/shared/repositories/CoastalApp/ALLBIN_INSTALL/NEMS-adcirc-atmesh-ww3data.x \
        --adcirc-processors 40
        --adcprep-executable /scratch2/COASTAL/coastal/save/shared/repositories/CoastalApp/ADCIRC/ALLBIN_INSTALL/adcprep \
        --modulefile /scratch2/COASTAL/coastal/save/shared/repositories/CoastalApp/modulefiles/envmodules_intel.hera \
        --forcings tidal,atmesh,ww3data \
        --tidal-source TPXO \
        --tidal-path /scratch2/COASTAL/coastal/save/shared/models/forcings/tides/h_tpxo9.v1.nc \
        --tidal-spinup-duration 12:06:00:00 \
        --atmesh-path /scratch2/COASTAL/coastal/save/shared/models/forcings/shinnecock/ike/wind_atm_fin_ch_time_vec.nc \
        --ww3data-path /scratch2/COASTAL/coastal/save/shared/models/forcings/shinnecock/ike/ww3.Constant.20151214_sxy_ike_date.nc

.. program-output:: initialize_adcirc -h

.. autofunction:: coupledmodeldriver.client.initialize_adcirc.initialize_adcirc

``generate_adcirc`` - generate configuration from JSON configuration files
--------------------------------------------------------------------------
.. code-block:: shell

    cd hera_shinnecock_ike_spinup_tidal_atmesh_ww3data
    generate_adcirc

.. program-output:: generate_adcirc -h

.. autofunction:: coupledmodeldriver.generate.adcirc.generate.generate_adcirc_configuration

``check_completion`` - check status of currently running model
--------------------------------------------------------------

.. code-block:: shell

    cd hera_shinnecock_ike_spinup_tidal_atmesh_ww3data
    check_completion
    check_completion spinup
    check_completion runs/*_13

.. program-output:: check_completion -h

.. autofunction:: coupledmodeldriver.client.check_completion.check_completion

``unqueued_runs`` - find and submit runs that haven't been queued to job manager
--------------------------------------------------------------------------------

.. code-block:: shell

    cd hera_shinnecock_ike_spinup_tidal_atmesh_ww3data
    unqueued_runs
    unqueued_runs --submit

.. program-output:: unqueued_runs -h

.. autofunction:: coupledmodeldriver.client.unqueued_runs.get_unqueued_runs
.. autofunction:: coupledmodeldriver.client.unqueued_runs.main
