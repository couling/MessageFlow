import typing as t
from abc import abstractmethod

__all__ = ['EncoderDecoder', 'EncoderContext', 'DecoderContext', 'StructFieldMap', 'StructDefinition', 'VariantSpec']


class EncoderContext(t.Protocol):
    """
    Writes objects to the underlying stream.  The EncoderContext is stateful and will automatically track which
    structures have already been declared and the ids of objects already encoded for use as back-refs.  The actual
    object encoding is done by various EncoderDecoder classes which can be found in encoder_decoder.
    """

    @abstractmethod
    def write(self, value: bytes):
        """
        Write raw bytes to the underlying output stream.  This is intended for use by custom types.
        :param value: The bytes to write
        """

    @abstractmethod
    def encode_object(self, value: t.Any, simple_form: bool = False):
        """
        Encode (and write) an object to the underlying output stream.  The object's type must be known to this encoder's
        schema.
        :param value:  An object of any type known to this encoder's schema.
        :param simple_form:  If True, only existing types may be encoded, no new struct definitions may be encoded
        """

    @abstractmethod
    def encode_variable_int(self, val: int):
        """
        Encode (and write) an integer to the underlying output stream.  This is the recommended way to send a integer
        as part of a custom data type where only an integer is expected.  Variable int does NOT encode a control-code.
        So recipients must expect to read one.

        This method should not be called directly applications wishing to encode integers.  Instead either __call__ or
        encode_object should be used as these will send a control code first to make sure the decoder knows what to
        decode.
        :param val: Any integer between 0 and 2**60-1
        """

    @abstractmethod
    def encode_string(self, value: str):
        """
        Encode (and write) a string to the underlying output stream in UTF-8.  This is the recommended way to send a
        string as part of a custom type where only a string is expected.  This does not send a control-code first so the
        decoder must be ready to expect a string.

        This method should not be called directly by applications wishing to encode integers.  Instead either __call__
        or encode_object should be used as these will send a control code first to make sure the decoder knows what to
        decode.
        :param value: Any string which can be represented in UTF-8
        :return:
        """

    def __call__(self, value: t.Any):
        """
        Send an object to the stream.  This is the "right" way for an application to encode an object onto the stream.
        :param value:  Any object who's type is known to the schema.
        """
        self.encode_object(value)


class DecoderContext(t.Protocol):
    """
    Reads objects from the underlying stream.  The DecoderContext is stateful and will automatically track which
    structures have already been declared and store a dict of objects already encoded such that back refs.  The actual
    object encoding is done by various EncoderDecoder classes which can be found in encoder_decoder.
    """

    @abstractmethod
    def read(self, byte_count: int) -> bytes:
        """
        Read raw bytes from the underlying stream.  This is intended for use by custom types.  This will return
        exactly the number of bytes requested or block until they are all available.  If the end of the input stream
        is reached an exception (message_stream.exceptions.UnexpectedEof) will be raised.
        :param byte_count:  The number of bytes to read.
        :return:  A bytes() object of exactly byte_count bytes.
        """

    @abstractmethod
    def decode_object(self, eof_okay=False, type_def_okay=True):
        """
        Decode an object from the underlying stream.  This is intended for use by custom types.
        :param eof_okay:  If true raises a StopIteration exception instead of message_stream.exceptions.UnexpectedEof.
        :param type_def_okay:  If False, will raise a ParseError if a type-def control-code is read immediately before
                               this object.
        :return: The object decoded from the stream
        """

    @abstractmethod
    def decode_variable_int(self) -> int:
        """
        (reads and) Decodes a variable int.  Since variable int does not have a control code first, it will just assume
        that whatever next on the stream is one.
        :return:  An integer between 0 and 2^^60-1
        """

    @abstractmethod
    def decode_string(self) -> str:
        """
        (reads and) Decodes a UTF-8 string.  Since this does not have a control-code first, it will just assume that
        whatever comes next on the stream is a valid string.
        :return: The Decoded string.
        """

    @abstractmethod
    def __iter__(self) -> t.Iterable[t.Any]:
        """
        The "right" way for an application to read objects from the stream is to iterate over it.
        :return:
        """


class StructFieldMap(t.NamedTuple):
    """
    Maps a structure field name onto python object fields.  Redundantly a different python attribute name can be used
    when encoding a python object to the one used when decoding the same object.  This is to account for __init__
    parameters not matching their respective attribute names.
    """
    encode_source: str  # Attribute name to read when encoding the object  (only known to this app)
    decode_target: str  # Parameter name to give when decoding the object  (only known to this app)
    name: str  # Given field name in the schema  (the name shared with other apps)


class StructDefinition(t.NamedTuple):
    """
    Defines a structure in the schema
    """
    encode_type: t.Type  # The type of object to encode  (only know to this app)
    decode_type: t.Type  # A function which will create the given object  (only known to this app)
    struct_name: str  # The name  (the name shared with other apps)
    fields: t.Collection[StructFieldMap]  # The fields in this structure


class VariantSpec(t.NamedTuple):
    """
    Defines how to encode a specific type-variant.
    This is exclusively for use as a return type for EncoderDecoder.select_variant
    """
    encode: t.Callable[[t.Any, EncoderContext], None]  # The method to call to encode this variant
    variant_key: t.Any  # The key for this variant.  Must be in EncoderDecoder.variants.
    allow_backref: bool  # If True the id of the object will be decoder


class EncoderDecoder(t.Protocol):
    """
    Represents an encoder-decoder for one specific python type.
    """
    variants: t.List[t.Any]  # A list of variants to this type, keys may be any indexable type including None.

    @abstractmethod
    def select_variant(self, value) -> VariantSpec:
        """
        Selects a variant to use to encode a specific value.  The response must include a callable to do the actual
        encoding but this may not necessarily be called if "allow_backref" is True.
        :param value: The value to select a variant for.
        """

    @abstractmethod
    def decode(self, variant: t.Any, source: DecoderContext) -> t.Any:
        """
        Decodes an object from source
        :param variant: The variant to be decoded
        :param source: The DecoderContext to use to read the object from
        :return:
        """
