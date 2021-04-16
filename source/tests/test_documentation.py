from pathlib import Path
from io import StringIO

from message_stream import default_schema


BASE_SCHEMA_DOC = Path(__file__).parent.parent.parent / "base_schema.md"


def test_base_schema_up_to_date():
    """
    Checks the documentation has been updated with the latest list of control codes.
    """
    buffer = StringIO()
    default_schema.document(buffer)
    with BASE_SCHEMA_DOC.open("rb") as file:
        assert buffer.getvalue().encode() == file.read()
