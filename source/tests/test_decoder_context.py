# pylint: disable=redefined-outer-name
import os
from io import BytesIO

import pytest

from message_stream import exceptions
from message_stream.encoder_decoder_context import DecoderContext


@pytest.fixture()
def encoded_data() -> BytesIO:
    return BytesIO()


@pytest.fixture()
def decoder_context(encoded_data) -> DecoderContext:
    return DecoderContext({}, {}, encoded_data)


def test_invalid_control_code_raises_parse_error(decoder_context: DecoderContext, encoded_data: BytesIO):
    encoded_data.write(bytes([99]))
    encoded_data.seek(0, os.SEEK_SET)
    with pytest.raises(exceptions.UnknownControlCode):
        _ = next(decoder_context)


def test_invalid_variable_int_raises_error(decoder_context: DecoderContext, encoded_data: BytesIO):
    encoded_data.write(bytes([0xF8, 0x00, 0x00]))
    encoded_data.seek(0, os.SEEK_SET)
    with pytest.raises(exceptions.ParseError):
        _ = next(decoder_context)


@pytest.mark.parametrize(('byte_value', 'expected_value'), [
    (bytes([0]), 0),
    (bytes([1]), 1),
    (bytes([0x7F]), 0x7F),
    (bytes([0x80, 0x00]), 0),  # Tolerant to bad encoders
    (bytes([0x81, 0x23]), 0x0123),
    (bytes([0xB1, 0x23]), 0x3123),
    (bytes([0xD1, 0x23, 0x45, 0x67]), 0x11234567),
    (bytes([0xEF, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE]), 0x0F123456789ABCDE),
], ids=lambda x: f"0x{x.hex()}" if isinstance(x, bytes) else str(x))
def test_read_variable_int_correct_value(byte_value: bytes, expected_value: int, decoder_context: DecoderContext,
                                         encoded_data: BytesIO):
    encoded_data.write(byte_value)
    encoded_data.seek(0, os.SEEK_SET)
    result = decoder_context.decode_variable_int()
    assert result == expected_value


@pytest.mark.parametrize('byte_value', [
    bytes([0]),
    bytes([1]),
    bytes([0x7F]),
    bytes([0x80, 0x00]),  # Tolerant to bad encoders
    bytes([0x81, 0x23]),
    bytes([0xB1, 0x23]),
    bytes([0xD1, 0x23, 0x45, 0x67]),
    bytes([0xEF, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE]),
], ids=lambda x: f"0x{x.hex()}")
def test_read_variable_int_does_not_over_read(byte_value: bytes, decoder_context: DecoderContext,
                                              encoded_data: BytesIO):
    encoded_data.write(byte_value)
    encoded_data.write(bytes(10))
    encoded_data.seek(0, os.SEEK_SET)
    _ = decoder_context.decode_variable_int()
    assert encoded_data.tell() == len(byte_value)
