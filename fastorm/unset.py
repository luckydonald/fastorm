from typing import Type


class UnsetType:
    pass
Unset = UnsetType()  # Used to indicate unset values in parameters or fields

UnsetType = Type[Unset]