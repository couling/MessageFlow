import typing as t

from . import abstract, constants, exceptions
from .abstract import StructDefinition, StructFieldMap
from .encoder_decoder import StructEncoderDecoder


__all__ = ['EncoderContext', 'DecoderContext']


class EncoderContext(abstract.EncoderContext):
    _encoders: t.Dict[t.Type, t.Tuple[abstract.EncoderDecoder, t.Dict[t.Any, int]]]
    _structures_in_schema: t.Dict[t.Type, StructDefinition]
    _target: t.BinaryIO
    _max_control_code: int
    _write_position: int
    _back_references: t.Dict[int, int]

    def __init__(self, encoders: t.Dict[t.Type, t.Tuple[abstract.EncoderDecoder, t.Dict[t.Any, int]]],
                 structures: t.Dict[t.Type, StructDefinition], target: t.BinaryIO):
        super().__init__()
        self._encoders = encoders.copy()
        self._structures_in_schema = structures.copy()
        self._target = target
        self._max_control_code = max(c for s in self._encoders.values() for c in s[1].values())
        self._back_references = {}

    def write(self, value: bytes):
        self._target.write(value)

    def encode_object(self, value: t.Any, simple_form: bool = False):
        try:
            encoder, variant_map = self._encoders[type(value)]
        except KeyError as ex:
            if simple_form or type(value) not in self._structures_in_schema:
                raise ValueError(f"Cannot encode unknown type {type(value)}") from ex
            self._declare_structure(type(value))
            encoder, variant_map = self._encoders[type(value)]

        encode_method, variant, allow_backref = encoder.select_variant(value)
        position = self._target.tell()
        if allow_backref and id(value) in self._back_references:
            self._encode_back_reference(value)
        else:
            control_code = variant_map[variant]
            self.encode_variable_int(control_code)
            encode_method(value, self)
            if allow_backref:
                self._back_references[id(value)] = position

    def encode_variable_int(self, val: int):
        if val < 0x80:
            self.write(val.to_bytes(1, constants.ENDIAN))
        elif val < 0x4000:
            self.write((val | 0x8000).to_bytes(2, constants.ENDIAN))
        elif val < 0x20000000:
            self.write((val | 0xC0000000).to_bytes(4, constants.ENDIAN))
        elif val < 0x1000000000000000:
            self.write((val | 0xE000000000000000).to_bytes(8, constants.ENDIAN))
        else:
            raise ValueError(f"Out of range {val}")

    def encode_string(self, value: str):
        v = value.encode("utf-8")
        self.encode_variable_int(len(v))
        self.write(v)

    def _encode_back_reference(self, value):
        offset = self._target.tell() - self._back_references[id(value)]
        self.encode_variable_int(constants.BACK_REF_CONTROL_CODE)
        self.encode_variable_int(offset)

    def _add_encoder(self, obj_type: t.Type, encoder: abstract.EncoderDecoder):
        variant_map = {}
        for variant in encoder.variants:
            self._max_control_code += 1
            variant_map[variant] = self._max_control_code
        self._encoders[obj_type] = encoder, variant_map

    def _declare_structure(self, struct_type: t.Type):
        struct_def = self._structures_in_schema[struct_type]
        encoder = StructEncoderDecoder(struct_def)
        self._add_encoder(struct_type, encoder)

        self.encode_variable_int(constants.STRUCT_DEF_CONTROL_CODE)
        self.encode_string(struct_def.struct_name)
        # The encoder may have several variants.  Make sure we send a control code for each one ...
        self.encode_variable_int(len(self._encoders[struct_type][1]))
        for variant, control_code in self._encoders[struct_type][1].items():
            self.encode_variable_int(control_code)
            self.encode_object(variant, simple_form=True)
        # Send the fields in the struct.  That way we never send the field names for every object.
        self.encode_variable_int(len(struct_def.fields))
        for field in struct_def.fields:
            self.encode_string(field.name)


class DecoderContext(abstract.DecoderContext):
    _decoders:  t.Dict[int, t.Tuple[abstract.EncoderDecoder, t.Any]]
    _source: t.BinaryIO
    _structures_in_schema: t.Dict[str, StructDefinition]
    _back_references = t.Dict[int, t.Any]

    def __init__(self, decoders: t.Dict[int, t.Tuple[abstract.EncoderDecoder, t.Any]],
                 structures: t.Dict[str, StructDefinition], source: t.BinaryIO):
        super().__init__()
        self._decoders = decoders.copy()
        self._structures_in_schema = structures.copy()
        self._source = source
        self._back_references = {}

    def read(self, byte_count: int) -> bytes:
        result = self._source.read(byte_count)
        if len(result) < byte_count:
            raise exceptions.UnexpectedEof()
        return result

    def decode_object(self, eof_okay=False, type_def_okay=True):
        while True:
            position = self._source.tell()
            try:
                control_code = self.decode_variable_int()
            except exceptions.UnexpectedEof as ex:
                if eof_okay and self._source.tell() == position:
                    raise StopIteration() from ex
                raise
            if control_code == constants.STRUCT_DEF_CONTROL_CODE:
                if not type_def_okay:
                    raise exceptions.ParseError("Attempt to define a new type at invalid location")
                self._declare_structure()
                continue
            if control_code == constants.BACK_REF_CONTROL_CODE:
                return self._decode_back_reference(position)
            try:
                decoder, variant = self._decoders[control_code]
            except KeyError as ex:
                raise exceptions.UnknownControlCode(control_code) from ex
            result = decoder.decode(variant, self)
            self._back_references[position] = result
            return result

    def decode_variable_int(self) -> int:
        first_byte = self.read(1)[0]
        if first_byte & 0x80 == 0:
            return first_byte
        if first_byte & 0xC0 == 0x80:
            return int.from_bytes(bytes((first_byte & 0x3F,)) + self.read(1), constants.ENDIAN)
        if first_byte & 0xE0 == 0xC0:
            return int.from_bytes(bytes((first_byte & 0x1F,)) + self.read(3), constants.ENDIAN)
        if first_byte & 0xF0 == 0xE0:
            return int.from_bytes(bytes((first_byte & 0x0F,)) + self.read(7), constants.ENDIAN)
        raise exceptions.ParseError(f"Invalid first byt for variable int {hex(first_byte)}")

    def decode_string(self) -> str:
        length = self.decode_variable_int()
        value = self.read(length)
        return value.decode("utf-8")

    def _decode_back_reference(self, current_position) -> t.Any:
        try:
            offset = self.decode_variable_int()
            return self._back_references[current_position - offset]
        except KeyError as ex:
            raise exceptions.ParseError("Invalid back reference") from ex

    def _add_decoder(self, decoder: abstract.EncoderDecoder, variants: t.Collection[t.Tuple[int, t.Any]]):
        for control_code, variant in variants:
            self._decoders[control_code] = decoder, variant

    def _declare_structure(self):
        # Decode the message
        struct_name = self.decode_string()
        variants = []
        for _ in range(self.decode_variable_int()):
            variants.append((self.decode_variable_int(), self.decode_object(type_def_okay=False)))
        fields = []
        for _ in range(self.decode_variable_int()):
            fields.append(self.decode_string())

        # Match the given fields to what we have in the schema
        # TODO implement strict mode
        try:
            struct_def = self._structures_in_schema[struct_name]
        except KeyError:
            # We didn't know about this one, let's just decode it to a dict
            struct_def = StructDefinition(type(None), dict, struct_name, tuple(StructFieldMap(f, f, f) for f in fields))
        else:
            expected_fields: t.Dict[str, StructFieldMap] = {f.name: f for f in struct_def.fields}
            # Fields
            struct_def = StructDefinition(struct_def.encode_type, struct_def.decode_type, struct_def.struct_name, tuple(
                StructFieldMap(expected_fields[f].encode_source, expected_fields[f].decode_target, f) for f in fields))

        # Allocate a decoder
        decoder = StructEncoderDecoder(struct_def)

        # Register the decoder
        self._add_decoder(decoder, variants)

    def __next__(self):
        return self.decode_object(eof_okay=True)

    def __iter__(self):
        return self
