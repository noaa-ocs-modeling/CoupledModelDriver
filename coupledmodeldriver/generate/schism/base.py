from os import PathLike
from typing import Any, List, Union

from nemspy.model import SCHISMEntry

from coupledmodeldriver.configure import Model, ModelJSON, SlurmJSON
from coupledmodeldriver.configure.base import AttributeJSON, NEMSCapJSON
from coupledmodeldriver.configure.configure import from_user_input
from coupledmodeldriver.configure.forcings.base import ForcingJSON

PYSCHISM_ATTRIBUTES = [
    # TODO enuemrate pySCHISM configuration attributes here
    ...
]
OUTPUT_INTERVAL_DEFAULTS = {
    ...
}


class SCHISMJSON(ModelJSON, NEMSCapJSON, AttributeJSON):
    """
    SCHISM configuration in ``configure_schism.json``

    stores a number of SCHISM parameters (..., etc.) and optionally NEMS parameters
    """

    name = 'SCHISM'
    default_filename = f'configure_schism.json'
    default_processors = 11
    default_attributes = PYSCHISM_ATTRIBUTES

    field_types = {
        # TODO write pySCHISM configuration fields here
        ...
    }

    def __init__(self, model: Model, **kwargs):
        super().__init__(model, **kwargs)
        self.__forcings = []
        self.__slurm_configuration = ...
        ...

    @property
    def forcings(self) -> List[ForcingJSON]:
        return list(self.__forcings)

    @forcings.setter
    def forcings(self, forcings: List[ForcingJSON]):
        if forcings is None:
            forcings = []
        for forcing in forcings:
            self.add_forcing(forcing)

    def add_forcing(self, forcing: ForcingJSON):
        if not isinstance(forcing, ForcingJSON):
            forcing = from_user_input(forcing)
        pyschism_forcing = forcing.pyschism_forcing

        existing_forcings = [
            existing_forcing.__class__.__name__ for existing_forcing in self.pyschism_forcings
        ]
        if pyschism_forcing.__class__.__name__ in existing_forcings:
            existing_index = existing_forcings.index(pyschism_forcing.__class__.__name__)
        else:
            existing_index = -1
        if existing_index > -1:
            self.__forcings[existing_index] = forcing
        else:
            self.__forcings.append(forcing)

    @property
    def slurm_configuration(self) -> List[SlurmJSON]:
        return self.__slurm_configuration

    @slurm_configuration.setter
    def slurm_configuration(self, slurm_configuration: SlurmJSON):
        if isinstance(slurm_configuration, ...):
            SlurmJSON.from_pyschism(slurm_configuration)
        self.__slurm_configuration = slurm_configuration

    @property
    def pyschism_forcings(self) -> List[...]:
        return [forcing.pyschism_forcing for forcing in self.forcings]

    @property
    def pyschism_mesh(self) -> ...:
        if self.__mesh is None:
            mesh = ...

            self.__mesh = mesh

        return self.__mesh

    @pyschism_mesh.setter
    def pyschism_mesh(self, pyschism_mesh: Union[..., PathLike]):
        self.__mesh = pyschism_mesh

    @property
    def pyschism_driver(self) -> ...:
        # TODO create a pySCHISM driver here
        return ...

    @property
    def nemspy_entry(self) -> SCHISMEntry:
        return SCHISMEntry(processors=self['processors'], **self['nems_parameters'])

    def __setitem__(self, key: str, value: Any):
        super().__setitem__(key, value)
        self.__mesh = None

    def __copy__(self) -> 'SCHISMJSON':
        instance = super().__copy__()
        instance.forcings = self.forcings
        return instance
