# pylint: disable=redefined-outer-name
from io import BytesIO

import pytest

from message_stream.encoder_decoder_context import EncoderContext
from message_stream.encoder_decoder import SentinelEncoder


@pytest.fixture()
def encoded_data() -> BytesIO:
    """
    Encoded data
    """
    return BytesIO()


@pytest.fixture()
def encoder_context(encoded_data) -> EncoderContext:
    """
    A minimal encoder_context without the usual EncoderDecoders.  It technically needs one so None has been added.
    """
    return EncoderContext({type(None): (SentinelEncoder(None), {None: 1})}, {}, encoded_data)


@pytest.mark.parametrize(('byte_value', 'encode_value'), [
    (bytes([0]), 0),
    (bytes([1]), 1),
    (bytes([0x7F]), 0x7F),
    (bytes([0x81, 0x23]), 0x0123),
    (bytes([0xB1, 0x23]), 0x3123),
    (bytes([0xD1, 0x23, 0x45, 0x67]), 0x11234567),
    (bytes([0xEF, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE]), 0x0F123456789ABCDE),
])
def test_encode_variable_int(byte_value: int, encode_value: int, encoder_context: EncoderContext,
                             encoded_data: BytesIO):
    """
    Check that encoding a variable int results in the correct number of bytes
    """
    encoder_context.encode_variable_int(encode_value)
    assert encoded_data.getvalue() == byte_value


def test_encode_variable_int_overflow(encoder_context: EncoderContext, encoded_data: BytesIO):
    """
    Check that a massive int will raise an exception not write junk
    :param encoder_context:
    :return:
    """
    with pytest.raises(ValueError):
        encoder_context.encode_variable_int(0x1000000000000000)
    assert encoded_data.tell() == 0


@pytest.mark.parametrize(('value', 'expected_result'), [
    ('', '\0'.encode("UTF-8")),
    ('hello', '\5hello'.encode("UTF-8")),
    ('£', '\2£'.encode("UTF-8")),
])
def test_encode_string(value: str, expected_result: bytes, encoder_context: EncoderContext, encoded_data: BytesIO):
    """
    Test that strings encode correctly
    """
    encoder_context.encode_string(value)
    result = encoded_data.getvalue()
    assert result == expected_result
