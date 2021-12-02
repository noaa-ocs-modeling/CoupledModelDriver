CLI Commands
============

``initialize_adcirc``
---------------------

``initialize_adcirc`` creates :ref:`json_configurations:JSON Configurations` from initial parameters of a model run (model start and end times, forcing files, storm track ID, etc.).
These JSON files provide a portable encapsulation of the entire model run, including model configurations in ``fort.15`` and NEMS couplings.
The files are read by :ref:`client:``generate_adcirc``` to generate the actual model configuration; ``generate_adcirc`` must be run again every time the JSON files change to keep the configuration up to date.

.. program-output:: initialize_adcirc -h

ADCIRC run options that are not exposed by this command, such as ``runs`` or ``gwce_solution_scheme``, can be specified by directly modifying the JSON files.

The following command creates JSON files for coupling ``(ATMESH + WW3DATA) -> ADCIRC`` over a small Shinnecock Inlet mesh:

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
        --adcprep-executable /scratch2/COASTAL/coastal/save/shared/repositories/CoastalApp/ALLBIN_INSTALL/adcprep \
        --modulefile /scratch2/COASTAL/coastal/save/shared/repositories/CoastalApp/modulefiles/envmodules_intel.hera \
        --forcings tidal,atmesh,ww3data \
        --tidal-source TPXO \
        --tidal-path /scratch2/COASTAL/coastal/save/shared/models/forcings/tides/h_tpxo9.v1.nc \
        --tidal-spinup-duration 12:06:00:00 \
        --atmesh-path /scratch2/COASTAL/coastal/save/shared/models/forcings/shinnecock/ike/wind_atm_fin_ch_time_vec.nc \
        --ww3data-path /scratch2/COASTAL/coastal/save/shared/models/forcings/shinnecock/ike/ww3.Constant.20151214_sxy_ike_date.nc

This will create the directory ``hera_shinnecock_ike_spinup_tidal_atmesh_ww3data/`` with the following JSON configuration files:

.. code-block::

    📂 hera_shinnecock_ike_spinup_tidal_atmesh_ww3data/
    ┣ 📜 configure_adcirc.json
    ┣ 📜 configure_atmesh.json
    ┣ 📜 configure_modeldriver.json
    ┣ 📜 configure_nems.json
    ┣ 📜 configure_slurm.json
    ┣ 📜 configure_tidal_forcing.json
    ┗ 📜 configure_ww3data.json

These files contain relevant configuration values for an ADCIRC run. You will likely wish to change these values to alter the
resulting run, before generating the actual model configuration. For instance, NEMS connections and the run sequence need to be
manually specified in ``configure_nems.json``.

``generate_adcirc``
-------------------

``generate_adcirc`` reads JSON files (created by :ref:`client:``initialize_adcirc```) and creates a set of ADCIRC configurations
(``fort.14``, ``fort.15``, etc.), as well as a script with which to submit the model run to a job manager such as Slurm.

.. program-output:: generate_adcirc -h

The following command will read the JSON files created in the example above and generate the following files:

.. code-block:: shell

    cd hera_shinnecock_ike_spinup_tidal_atmesh_ww3data
    generate_adcirc

.. code-block::

    📂 hera_shinnecock_ike_spinup_tidal_atmesh_ww3data/
    ┣ 📜 configure_adcirc.json
    ┣ 📜 configure_atmesh.json
    ┣ 📜 configure_modeldriver.json
    ┣ 📜 configure_nems.json
    ┣ 📜 configure_slurm.json
    ┣ 📜 configure_tidal_forcing.json
    ┣ 📜 configure_ww3data.json
    ┣ 📂 spinup/
    ┃  ┣ 📜 fort.13
    ┃  ┣ 🔗 fort.14 -> ../fort.14
    ┃  ┣ 📜 fort.15
    ┃  ┣ 📜 nems.configure
    ┃  ┣ 📜 model_configure
    ┃  ┣ 🔗 atm_namelist.rc -> ./model_configure
    ┃  ┣ 📜 config.rc
    ┃  ┣ 📜 setup.job
    ┃  ┗ 📜 adcirc.job
    ┣ 📂 runs/
    ┃  ┗ 📂 unperturbed/
    ┃    ┣ 📜 fort.13
    ┃    ┣ 🔗 fort.14 -> ../../fort.14
    ┃    ┣ 📜 fort.15
    ┃    ┣ 🔗 fort.67.nc -> ../../spinup/fort.67.nc
    ┃    ┣ 🔗 fort.68.nc -> ../../spinup/fort.68.nc
    ┃    ┣ 📜 nems.configure
    ┃    ┣ 📜 model_configure
    ┃    ┣ 🔗 atm_namelist.rc -> ./model_configure
    ┃    ┣ 📜 config.rc
    ┃    ┣ 📜 setup.job
    ┃    ┗ 📜 adcirc.job
    ┣ 📜 fort.14
    ┣ 📜 cleanup.sh
    ┗ 📜 run_hera.sh

``check_completion``
--------------------

``check_completion`` checks the completion status of a running model directory.

.. program-output:: check_completion -h

.. code-block:: shell

    check_completion

.. code-block:: json

    {
        "hera_shinnecock_ike_spinup_tidal_atmesh_ww3data": {
            "spinup": "running - 15%",
            "runs": "not_started - 0%"
        }
    }

you can also pass a specific directory (or several directories):

.. code-block:: shell

    check_completion spinup

.. code-block:: json

    {
        "spinup": "running - 27%"
    }

.. code-block:: shell

    cd run_20211027_florence_besttrack_250msubset_quadrature
    check_completion runs/*_13

.. code-block:: json

    {
        "vortex_4_variable_perturbation_13": "completed - 100.0%",
        "vortex_4_variable_quadrature_13": "not_started - 0%"
    }

if a run has an error, you can pass `--verbose` to see detailed logs:

.. code-block:: shell

    check_completion spinup

.. code-block:: json

    {
        "spinup": "error - 0%"
    }

.. code-block:: shell

    check_completion spinup --verbose

.. code-block:: json

    {
        "spinup": {
            "status": "error",
            "progress": "0%",
            "error": {
                "ADCIRC_SETUP_SPINUP.err.log": [
                    "forrtl: severe (24): end-of-file during read, unit -4, file /proc/92195/fd/0\n",
                    "Image              PC                Routine            Line        Source             \n",
                    "adcprep            000000000069A72E  Unknown               Unknown  Unknown\n",
                    "adcprep            00000000006CBAAF  Unknown               Unknown  Unknown\n",
                    "adcprep            000000000050A5CB  openprepfiles_           6996  prep.F\n",
                    "adcprep            0000000000507F22  prep13_                   753  prep.F\n",
                    "adcprep            000000000042E2E9  prepinput_                717  adcprep.F\n",
                    "adcprep            000000000042BCDB  MAIN__                    239  adcprep.F\n",
                    "adcprep            000000000040B65E  Unknown               Unknown  Unknown\n",
                    "libc-2.17.so       00002AAEC02EB555  __libc_start_main     Unknown  Unknown\n",
                    "adcprep            000000000040B569  Unknown               Unknown  Unknown\n",
                    "srun: error: h24c51: task 0: Exited with exit code 24\n",
                    "srun: launch/slurm: _step_signal: Terminating StepId=25366266.1\n"
                ]
            }
        }
    }

.. code-block:: shell

    check_completion runs

.. code-block:: json

    {
        "spinup": "failed - 0%"
    }

.. code-block:: shell

    check_completion runs --verbose

.. code-block:: json

    {
        "runs": {
            "status": "failed",
            "progress": "0%",
            "failed": {
                "fort.16": "ADCIRC output file `fort.16` not found"
            },
            "error": {
                "ADCIRC_SETUP_unperturbed.err.log": [
                    "slurmstepd: error: execve(): /scratch2/COASTAL/coastal/save/shared/repositories/CoastalApp/ADCIRC/ALLBIN_INSTALL/adcprep: No such file or directory\n",
                    "srun: error: h18c49: task 0: Exited with exit code 2\n",
                    "srun: launch/slurm: _step_signal: Terminating StepId=25366268.0\n"
                ]
            }
        }
    }

``unqueued_runs``
-----------------

``unqueued_runs`` finds and optionally submits runs that haven't been queued to a job manager.

.. program-output:: unqueued_runs -h

corresponding Python functions
------------------------------

.. autofunction:: coupledmodeldriver.client.initialize_adcirc.initialize_adcirc
.. autofunction:: coupledmodeldriver.generate.adcirc.generate.generate_adcirc_configuration
.. autofunction:: coupledmodeldriver.client.check_completion.check_completion
.. autofunction:: coupledmodeldriver.client.unqueued_runs.get_unqueued_runs
