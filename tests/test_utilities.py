from coupledmodeldriver.utilities import create_symlink
from tests import INPUT_DIRECTORY, OUTPUT_DIRECTORY, REFERENCE_DIRECTORY


def test_create_symlink():
    input_filename = INPUT_DIRECTORY / 'symlink_test.txt'
    output_filename = OUTPUT_DIRECTORY / input_filename.name
    reference_filename = REFERENCE_DIRECTORY / input_filename.name

    create_symlink(input_filename, output_filename)

    with open(output_filename) as output_file:
        with open(reference_filename) as reference_file:
            assert output_file.read() == reference_file.read()
