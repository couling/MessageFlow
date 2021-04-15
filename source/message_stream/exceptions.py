class ParseError(Exception):
    pass


class UnexpectedEof(ParseError):
    pass


class UnknownControlCode(ParseError):
    def __init__(self, control_code: int):
        super().__init__(f"Unknown control_code {control_code}")
