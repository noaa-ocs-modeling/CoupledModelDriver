from os import PathLike
from pathlib import Path, PurePosixPath

from ..adcirc import ADCIRCGenerationScript
from ..job_scripts import Script


class AdcircNEMSSetupScript(Script):
    """ script for setting up ADCIRC NEMS configuration """

    def __init__(
        self,
        nems_configure_filename: PathLike,
        model_configure_filename: PathLike,
        config_rc_filename: PathLike,
        fort67_filename: PathLike = None,
    ):
        self.nems_configure_filename = PurePosixPath(nems_configure_filename)
        self.model_configure_filename = PurePosixPath(model_configure_filename)
        self.config_rc_filename = PurePosixPath(config_rc_filename)
        self.fort67_filename = (
            PurePosixPath(fort67_filename) if fort67_filename is not None else None
        )

        commands = [
            f'ln -sf {self.nems_configure_filename} ./nems.configure',
            f'ln -sf {self.model_configure_filename} ./model_configure',
            f'ln -sf {self.config_rc_filename} ./config.rc',
            f'ln -sf ./model_configure ./atm_namelist.rc',
        ]

        if self.fort67_filename is not None:
            commands.extend(['', f'ln -sf {self.fort67_filename} ./fort.67.nc'])

        super().__init__(commands)


class NEMSADCIRCGenerationScript(ADCIRCGenerationScript):
    def __str__(self):
        lines = [
            'from pathlib import Path',
            '',
            'from coupledmodeldriver.adcirc.nems_adcirc import generate_nems_adcirc_configuration',
            '',
            '',
            "if __name__ == '__main__':",
            '    generate_nems_adcirc_configuration(output_directory=Path(__file__).parent, overwrite=True)',
        ]

        return '\n'.join(lines)

    def write(self, filename: PathLike, overwrite: bool = False):
        if not isinstance(filename, Path):
            filename = Path(filename)

        if filename.is_dir():
            filename = filename / f'generate_nems_adcirc.py'

        super().write(filename, overwrite)
