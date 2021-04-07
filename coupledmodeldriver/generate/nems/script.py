from os import PathLike
from pathlib import Path

from ..adcirc import ADCIRCGenerationScript


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
