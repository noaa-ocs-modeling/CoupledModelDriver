from os import PathLike

from coupledmodeldriver.configure import CirculationModelJSON
from coupledmodeldriver.configure.base import AttributeJSON, NEMSCapJSON

SCHISM_ATTRIBUTES = {}


class SCHISMJSON(CirculationModelJSON, NEMSCapJSON, AttributeJSON):
    name = 'ADCIRC'
    default_filename = f'configure_adcirc.json'
    default_processors = 11
    default_attributes = SCHISM_ATTRIBUTES

    def __init__(self, executable: PathLike, **kwargs):
        super().__init__(executable, **kwargs)
