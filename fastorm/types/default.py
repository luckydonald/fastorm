from typing import TypeVar


class UseDefault:
    pass
    pass
UseDefaultType = TypeVar("UseDefaultType", bound=UseDefault)
UseDefault = UseDefault()
