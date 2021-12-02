JSON Configurations
===================

``coupledmodeldriver`` generates and reads various modular JSON files to store an overall configuration.
These JSON files can be manually edited, swapped in and out, or removed entirely to modify the resulting configuration.

For example, consider the following simple NEMS-ADCIRC configuration:

- ``configure_modeldriver.json``
- ``configure_nems.json``
- ``configure_adcirc.json``

This can be turned into an ADCIRC-only configuration, that ingests tidal forcing and uses Slurm, by adding ``configure_tidal.json`` and ``configure_slurm.json``, and removing ``configure_nems.json``:

- ``configure_modeldriver.json``
- ``configure_slurm.json``
- ``configure_adcirc.json``
- ``configure_tidal.json``

Then, the configuration can be regenerated with ``generate_adcirc``.

``configure_modeldriver.json``
------------------------------

.. autoclass:: coupledmodeldriver.configure.base.ModelDriverJSON

``configure_slurm.json``
------------------------

.. autoclass:: coupledmodeldriver.configure.base.SlurmJSON

``configure_nems.json``
-----------------------

.. autoclass:: coupledmodeldriver.configure.base.NEMSJSON

``configure_adcirc.json``
-------------------------

.. autoclass:: coupledmodeldriver.generate.adcirc.base.ADCIRCJSON

``configure_tidal.json``
------------------------

.. autoclass:: coupledmodeldriver.configure.forcings.base.TidalForcingJSON

``configure_besttrack.json``
----------------------------

.. autoclass:: coupledmodeldriver.configure.forcings.base.BestTrackForcingJSON

``configure_owi.json``
----------------------

.. autoclass:: coupledmodeldriver.configure.forcings.base.OWIForcingJSON

``configure_atmesh.json``
-------------------------

.. autoclass:: coupledmodeldriver.configure.forcings.base.ATMESHForcingJSON

``configure_ww3data.json``
--------------------------

.. autoclass:: coupledmodeldriver.configure.forcings.base.WW3DATAForcingJSON

abstract classes
----------------

configuration
^^^^^^^^^^^^^

.. autoclass:: coupledmodeldriver.configure.base.ConfigurationJSON
.. autoclass:: coupledmodeldriver.configure.base.AttributeJSON

NEMS
^^^^

.. autoclass:: coupledmodeldriver.configure.base.NEMSCapJSON

model
^^^^^

.. autoclass:: coupledmodeldriver.configure.models.ModelJSON
