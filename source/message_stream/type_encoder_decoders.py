import datetime
import decimal
import struct
import typing as t
from abc import ABC, abstractmethod

import pytz

from .abstract import EncoderDecoder, VariantSpec, EncoderContext, DecoderContext, StructDefinition
from .constants import ENDIAN, SKIP
from .exceptions import ParseError


class SingleVariantEncoder(EncoderDecoder, ABC):
    variants = [None]
    _supports_back_ref = True

    def select_variant(self, value) -> VariantSpec:
        return VariantSpec(self._encode, None, self._supports_back_ref)

    @abstractmethod
    def _encode(self, value, target: EncoderContext):
        pass


class SentinelEncoder(SingleVariantEncoder):
    _supports_back_ref = False

    def __init__(self, sentinel_value):
        super().__init__()
        self._sentinel_value = sentinel_value

    def _encode(self, value, target: EncoderContext):
        pass

    def decode(self, variant: t.Any, source: DecoderContext) -> t.Any:
        return self._sentinel_value


class BoolEncoderDecoder(EncoderDecoder):
    variants = [False, True]

    def select_variant(self, value) -> VariantSpec:
        return VariantSpec(self.encode, bool(value), False)

    def encode(self, _, target: EncoderContext):
        pass

    def decode(self, variant: bool, source: DecoderContext) -> bool:
        return variant


class IntEncoderDecoder(EncoderDecoder):
    variants = [1, 2, 4, 8, ...]

    def select_variant(self, value: int) -> VariantSpec:
        length = int((7 + value.bit_length()) / 8)
        if length == 1:
            return VariantSpec(self._encode_1_byte, 1, False)
        if length == 2:
            return VariantSpec(self._encode_2_byte, 2, False)
        if length <= 4:
            return VariantSpec(self._encode_4_byte, 4, False)
        if length <= 8:
            return VariantSpec(self._encode_8_byte, 8, False)
        return VariantSpec(self._encode_big_byte, ..., True)

    @staticmethod
    def _encode_1_byte(value: int, target: EncoderContext):
        target.write(value.to_bytes(1, ENDIAN))

    @staticmethod
    def _encode_2_byte(value: int, target: EncoderContext):
        target.write(value.to_bytes(2, ENDIAN))

    @staticmethod
    def _encode_4_byte(value: int, target: EncoderContext):
        target.write(value.to_bytes(4, ENDIAN))

    @staticmethod
    def _encode_8_byte(value: int, target: EncoderContext):
        target.write(value.to_bytes(8, ENDIAN))

    @staticmethod
    def _encode_big_byte(value: int, target: EncoderContext):
        byte_length = int((7+value.bit_length())/8)
        target.encode_variable_int(byte_length)
        target.write(value.to_bytes(byte_length, ENDIAN))

    def decode(self, variant: t.Any, source: DecoderContext) -> int:
        if variant is ...:
            variant = source.decode_variable_int()
        value = source.read(variant)
        return int.from_bytes(value, ENDIAN)


class BytesEncoderDecoder(SingleVariantEncoder):

    def _encode(self, value, target: EncoderContext):
        target.encode_variable_int(len(value))
        target.write(value)

    def decode(self, variant: t.Any, source: DecoderContext) -> t.Any:
        length = source.decode_variable_int()
        return source.read(length)


class StringEncoderDecoder(EncoderDecoder):
    variants = [1, 0, ...]
    ENCODING = "utf8"

    def select_variant(self, value) -> VariantSpec:
        if len(value) == 0:
            return VariantSpec(self._encode_fixed, 0, False)
        if len(value) == 1:
            return VariantSpec(self._encode_fixed, 1, False)
        return VariantSpec(self._encode, ..., True)

    @staticmethod
    def _encode_fixed(value, target: EncoderContext):
        content = value.encode("utf-8")
        target.write(content)

    def _encode(self, value, target: EncoderContext):
        content = value.encode(self.ENCODING)
        target.encode_variable_int(len(content))
        target.write(content)

    def decode(self, variant: t.Any, source: DecoderContext) -> t.Any:
        if variant == 0:
            return ""
        if variant == 1:
            bytes_read = source.read(1)
            if bytes_read[0] & 0x80:
                if bytes_read[0] & 0xE0 == 0xC0:
                    bytes_read += source.read(1)
                elif bytes_read[0] & 0xF0 == 0xE0:
                    bytes_read += source.read(2)
                elif bytes_read[0] & 0xF8 == 0xF0:
                    bytes_read += source.read(3)
                else:
                    raise ParseError(f"Invalid UTF-8 first byte 0x{hex(bytes_read[0])}")
            return bytes_read.decode(self.ENCODING)
        content_length = source.decode_variable_int()
        content = source.read(content_length)
        return content.decode(self.ENCODING)


class FloatEncoder(SingleVariantEncoder):
    _STRUCT = struct.Struct('d')

    def _encode(self, value, target: EncoderContext):
        target.write(self._STRUCT.pack(value))

    def decode(self, variant: t.Any, source: DecoderContext) -> t.Any:
        result, = self._STRUCT.unpack(source.read(self._STRUCT.size))
        return result


class DecimalEncoder(EncoderDecoder):
    variants = [1, -1]
    _DECODE_MAP = "0123456789."
    _ENCODE_MAP = {key: value for value, key in enumerate(_DECODE_MAP)}

    def select_variant(self, value: decimal.Decimal) -> VariantSpec:
        if value < 0:
            return VariantSpec(self._encode, -1, False)
        return VariantSpec(self._encode, 1, False)

    def _encode(self, value: decimal.Decimal, target: EncoderContext):
        def _encode_iter():
            try:
                iterator = iter(string_value)
                while True:
                    i = next(iterator)
                    j = next(iterator)
                    yield self._ENCODE_MAP[i] << 4 | self._ENCODE_MAP[j]
            except StopIteration:
                if len(string_value) % 2:
                    yield self._ENCODE_MAP[string_value[-1]] << 4 | 0x0F

        if value < 0:
            value = 0 - value
        string_value = str(value)
        target.encode_variable_int(len(string_value))
        target.write(bytes(_encode_iter()))

    def decode(self, variant: t.Any, source: DecoderContext) -> t.Any:
        def decode_iter():
            byte_val = 0
            try:
                for byte_val in bytes_read:
                    yield self._DECODE_MAP[(byte_val & 0xF0) >> 4]
                    yield self._DECODE_MAP[byte_val & 0x0F]
            except IndexError as ex:
                if byte_val & 0x0F != 0x0F:
                    raise ParseError(f"Unexpected byte value in decimal {hex(byte_val)}") from ex
        length = source.decode_variable_int()
        bytes_read = source.read(int(length / 2) + (length % 2))
        result = decimal.Decimal(''.join(decode_iter()))
        return result * variant


class DatetimeEncoder(EncoderDecoder):
    variants = ['iso', 'iana']

    def select_variant(self, value: datetime.datetime) -> VariantSpec:
        if isinstance(value.tzinfo, pytz.tzinfo.BaseTzInfo) and str(value.tzinfo) in pytz.all_timezones_set:
            return VariantSpec(self._encode_iana, 'iana', True)
        return VariantSpec(self._encode_iso, 'iso', True)

    @staticmethod
    def _encode_iso(value: datetime.datetime, encoder: EncoderContext):
        encoder.encode_string(value.isoformat())

    @staticmethod
    def _encode_iana(value: datetime.datetime, encoder: EncoderContext):
        timezone = value.tzinfo
        timezone_string = str(timezone)
        timestamp_string = value.astimezone(datetime.timezone.utc).replace(tzinfo=None).isoformat()
        encoder.encode_string(timestamp_string)
        encoder.encode_string(timezone_string)

    def decode(self, variant: t.Any, source: DecoderContext) -> t.Any:
        timestamp_string = source.decode_string()
        result = datetime.datetime.fromisoformat(timestamp_string)
        if variant == 'iana':
            timezone_string = source.decode_string()
            timezone = pytz.timezone(timezone_string)
            result = result.replace(tzinfo=datetime.timezone.utc).astimezone(timezone)
        return result


class SequenceElementEncoder(SingleVariantEncoder):
    def __init__(self, sequence_factory):
        super().__init__()
        self._sequence_factory = sequence_factory

    def _encode(self, value, target: EncoderContext):
        target.encode_variable_int(len(value))
        for item in value:
            target.encode_object(item)

    def decode(self, variant: t.Any, source: DecoderContext) -> t.Any:
        item_count = source.decode_variable_int()
        result = self._sequence_factory(source.decode_object() for _ in range(item_count))
        return result


class DictEncoderDecoder(SingleVariantEncoder):
    _dict_factory = t.Callable[[t.Iterable[t.Tuple[t.Any, t.Any]]], t.Any]

    def __init__(self, dict_factory=dict):
        super().__init__()
        self._dict_factory = dict_factory

    def _encode(self, value, target: EncoderContext):
        target.encode_variable_int(len(value))
        for k, v in value.items():
            target.encode_object(k)
            target.encode_object(v)

    def decode(self, variant: t.Any, source: DecoderContext) -> t.Any:
        item_count = source.decode_variable_int()
        values = ((source.decode_object(), source.decode_object()) for _ in range(item_count))
        return self._dict_factory(values)


class StructEncoderDecoder(SingleVariantEncoder):

    def __init__(self, struct_def: StructDefinition):
        super().__init__()
        self._struct_def = struct_def

    def _encode(self, value, target: EncoderContext):
        for field in self._struct_def.fields:
            target.encode_object(getattr(value, field.encode_source, SKIP))

    def decode(self, variant: t.Any, source: DecoderContext) -> t.Any:
        values = {}
        for field in self._struct_def.fields:
            v = source.decode_object()
            if v is not SKIP and field.decode_target is not SKIP:
                values[field.decode_target] = v
        return self._struct_def.decode_type(**values)
