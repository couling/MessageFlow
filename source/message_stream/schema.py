import dataclasses
import datetime
import decimal
import functools
import io
import typing as t

from . import encoder_decoder, constants
from .abstract import StructDefinition, StructFieldMap, EncoderDecoder
from .encoder_decoder_context import EncoderContext, DecoderContext

__all__ = ['Schema', 'default_schema', 'dump_bytes', 'load_bytes']


class Schema:
    _encoders: t.Dict[t.Type, t.Tuple[EncoderDecoder, t.Dict[t.Any, int]]]
    _decoders: t.Dict[int, t.Tuple[EncoderDecoder, t.Any]]
    _structures_by_type: t.Dict[t.Type, StructDefinition]
    _structures_by_name: t.Dict[str, StructDefinition]

    def __init__(self, parent: "Schema" = None):
        if parent is None:
            try:
                parent = default_schema
            except NameError:
                # Bootstrap.  When creating default_schema, it will not already exist!
                self._encoders = {}
                self._decoders = {}
                self._structures_by_name = {}
                self._structures_by_type = {}
                return
        self._encoders = parent._encoders.copy()
        self._decoders = parent._decoders.copy()
        self._structures_by_name = parent._structures_by_name.copy()
        self._structures_by_type = parent._structures_by_type.copy()

    def decoder(self, source: t.BinaryIO) -> t.Union[t.Iterable[t.Any], t.Iterator[t.Any]]:
        return DecoderContext(self._decoders.copy(), self._structures_by_name, source)

    def encoder(self, target: t.BinaryIO) -> t.Callable[[t.Any], None]:
        return EncoderContext(self._encoders, self._structures_by_type, target)

    def dump_bytes(self, value: t.Any) -> bytes:
        buffer = io.BytesIO()
        encode = self.encoder(buffer)
        encode(value)
        return buffer.getvalue()

    def load_bytes(self, buffer: bytes):
        buffer = io.BytesIO(buffer)
        decoder = self.decoder(buffer)
        return next(decoder)

    def add_type(self, object_type: t.Type, encoder: EncoderDecoder,
                 control_codes: t.Union[int, t.Iterable[int], None] = None):
        if control_codes is None:
            try:
                max_control_code = max(self._decoders) + 1
            except ValueError:
                max_control_code = 9
            control_codes = range(max_control_code, max_control_code + len(encoder.variants))
        elif isinstance(control_codes, int):
            control_codes = (control_codes,)
        variant_map = dict(zip(encoder.variants, control_codes))
        if len(variant_map) != len(encoder.variants):
            raise ValueError(f"{str(encoder)} has {len(encoder.variants)} but only {len(variant_map)} "
                             f"control-codes were given")
        if len(set(variant_map.values())) != len(variant_map.values()):
            raise ValueError("Duplicate control_codes were given")
        for control_code in variant_map.values():
            if control_code in self._decoders:
                raise ValueError(f"Control code {control_code} already defined")
        for variant, control_code in variant_map.items():
            self._decoders[control_code] = encoder, variant
        self._encoders[object_type] = encoder, variant_map

    def define_structure(self, _type_def: t.Type = ..., name: t.Union[str, t.Callable[[t.Type], str]] = None):
        @functools.wraps(_type_def)
        def wrapper(type_def_2):
            self.define_structure(type_def_2, name)
            return type_def_2

        if _type_def is ...:
            # This function has been called to generate a decorator
            return wrapper

        def eval_name(type_class):
            if hasattr(name, '__call__'):
                # A naming function has been provided.  It can decline to give a name returning None
                result = name(type_class)
                if result is not None:
                    return result
            if type_class is _type_def and isinstance(name, str):
                # A single string name was provided for this structure (only this one, not it's children)
                return name
            if type_class in new_structures_by_type:
                # Don't auto-name something we already have a name for
                return new_structures_by_type[type_class][0]
            # All other options have failed so auto-name the structure
            return type_class.__name__

        new_structures_by_name = self._structures_by_name.copy()
        new_structures_by_type = self._structures_by_type.copy()
        for new_type, fields in self._evaluate_struct_schema(_type_def).items():
            new_name = eval_name(new_type)
            if new_type in new_structures_by_type and new_name != new_structures_by_type[new_type][0]:
                del new_structures_by_name[new_structures_by_type[new_type][0]]
            if new_name in new_structures_by_name:
                raise ValueError(f"Duplicate struct name {name} for types {new_type} and "
                                 f"{self._structures_by_name[name][0]}")
            struct_def = StructDefinition(new_type, new_type, new_name, tuple(StructFieldMap(f, f, f) for f in fields))
            new_structures_by_type[new_type] = struct_def
            new_structures_by_name[new_name] = struct_def
        self._structures_by_type = new_structures_by_type
        self._structures_by_name = new_structures_by_name

    def _evaluate_struct_schema(self, structure: t.Type) -> t.Dict[t.Type, t.List[str]]:
        results: t.Dict[t.Type, t.List[str]] = {}
        already_evaluated = set()
        to_evaluate = {structure}
        while to_evaluate:
            item = to_evaluate.pop()
            already_evaluated.add(id(item))
            if item in self._encoders:
                pass
            elif dataclasses.is_dataclass(item):
                results[item] = self._evaluate_dataclass_struct(item, to_evaluate, already_evaluated)
            elif isinstance(item, type) and issubclass(item, tuple) and hasattr(item, '_fields'):
                results[item] = self._evaluate_namedtuple_struct(item, to_evaluate, already_evaluated)
            else:
                origin = t.get_origin(item)
                if origin is not None and (origin is t.Union or origin in self._encoders):
                    # This is things like typing.List, typing.Dict.
                    # We accept any of those which are surrogates for things we have as encoders
                    self._evaluate_typing_struct(item, to_evaluate, already_evaluated)
                else:
                    raise TypeError(f"Cannot evaluate structure for type {item}, must be @dataclass or NamedTuple")
        return results

    @classmethod
    def _evaluate_dataclass_struct(cls, eval_struct, to_evaluate: t.Set[type],
                                   already_evaluated: t.Set[int]) -> t.List[str]:
        field_names = []
        for field in dataclasses.fields(eval_struct):
            field_names.append(field.name)
            cls._evaluate_child(field.type, to_evaluate, already_evaluated)
        return field_names

    @classmethod
    def _evaluate_namedtuple_struct(cls, eval_struct, to_evaluate: t.Set[type],
                                    already_evaluated: t.Set[int]) -> t.List[str]:
        field_names = []
        # pylint: disable=protected-access
        for field in eval_struct._fields:
            field_names.append(field)
            if hasattr(eval_struct, '_field_types'):
                field_type = eval_struct._field_types.get(field)
                cls._evaluate_child(field_type, to_evaluate, already_evaluated)
        return field_names

    @classmethod
    def _evaluate_typing_struct(cls, child: type, to_evaluate: t.Set[type], already_evaluated: t.Set[int]):
        for field in t.get_args(child):
            cls._evaluate_child(field, to_evaluate, already_evaluated)

    @staticmethod
    def _evaluate_child(child: type, to_evaluate: t.Set[type], already_evaluated: t.Set[int]):
        if id(child) not in already_evaluated:
            to_evaluate.add(child)


default_schema = Schema()

default_schema.add_type(type(constants.SKIP), encoder_decoder.SentinelEncoder(constants.SKIP))
default_schema.add_type(type(None), encoder_decoder.SentinelEncoder(None))
default_schema.add_type(type(...), encoder_decoder.SentinelEncoder(...))
default_schema.add_type(bool, encoder_decoder.BoolEncoderDecoder())
default_schema.add_type(int, encoder_decoder.IntEncoderDecoder())
default_schema.add_type(bytes, encoder_decoder.BytesEncoderDecoder())
default_schema.add_type(str, encoder_decoder.StringEncoderDecoder())
default_schema.add_type(float, encoder_decoder.FloatEncoder())
default_schema.add_type(decimal.Decimal, encoder_decoder.DecimalEncoder())
default_schema.add_type(datetime.datetime, encoder_decoder.DatetimeEncoder())
default_schema.add_type(tuple, encoder_decoder.SequenceElementEncoder(tuple))
default_schema.add_type(list, encoder_decoder.SequenceElementEncoder(list))
default_schema.add_type(set, encoder_decoder.SequenceElementEncoder(set))
default_schema.add_type(dict, encoder_decoder.DictEncoderDecoder())


def dump_bytes(value: t.Any) -> bytes:
    return default_schema.dump_bytes(value)


def load_bytes(buffer: bytes):
    return default_schema.load_bytes(buffer)
