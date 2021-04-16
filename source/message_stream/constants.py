ENDIAN = 'big'

STRUCT_DEF_CONTROL_CODE = 0
BACK_REF_CONTROL_CODE = 1


# pylint: disable=too-few-public-methods
class SkipType:
    """
    Used as a sentinel indicate a field should be skipped
    """


SKIP = SkipType()
