class ParseError(Exception):
    """
    General parse error.  While decoding, most errors should decend from this class
    """


class UnexpectedEof(ParseError):
    """
    The end of the stream was reached mid-way through an object.  This usually means a file got truncated or a
    connection closed unexpectedly.
    """


class UnknownControlCode(ParseError):
    """
    While decoding, a control code was sent which has not been registered.
    """
    def __init__(self, control_code: int):
        super().__init__(f"Unknown control_code {control_code}")
