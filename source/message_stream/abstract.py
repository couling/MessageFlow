import typing as t
from abc import abstractmethod

__all__ = ['EncoderDecoder', 'EncoderContext', 'DecoderContext', 'StructFieldMap', 'StructDefinition', 'VariantSpec']


class EncoderContext(t.Protocol):

    @abstractmethod
    def write(self, value: bytes):
        pass

    @abstractmethod
    def encode_object(self, value: t.Any, simple_form: bool = False):
        pass

    @abstractmethod
    def encode_variable_int(self, val: int):
        pass

    @abstractmethod
    def encode_string(self, value: str):
        pass

    def __call__(self, value: t.Any):
        self.encode_object(value)


class DecoderContext(t.Protocol):

    @abstractmethod
    def read(self, byte_count: int) -> bytes:
        pass

    @abstractmethod
    def decode_object(self, eof_okay=False, type_def_okay=True):
        pass

    @abstractmethod
    def decode_variable_int(self) -> int:
        pass

    @abstractmethod
    def decode_string(self) -> str:
        pass

    @abstractmethod
    def __iter__(self):
        pass


class StructFieldMap(t.NamedTuple):
    encode_source: str
    decode_target: str
    name: str


class StructDefinition(t.NamedTuple):
    encode_type: t.Type
    decode_type: t.Type
    struct_name: str
    fields: t.Collection[StructFieldMap]


class VariantSpec(t.NamedTuple):
    encode: t.Callable[[t.Any, EncoderContext], None]
    variant_key: t.Any
    allow_backref: bool


class EncoderDecoder(t.Protocol):
    variants: t.List[t.Any]

    @abstractmethod
    def select_variant(self, value) -> VariantSpec:
        pass

    @abstractmethod
    def decode(self, variant: t.Any, source: DecoderContext) -> t.Any:
        pass
