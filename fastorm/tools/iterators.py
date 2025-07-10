from typing import Iterable

from ..types.basics import AType, MaybeTuple


def must_be_none_or_one(items: Iterable[AType]) -> AType | None:
    items = list(items)  # TODO: iterator is more efficient, we could check for a second loop item
    assert len(items) <= 1, f"Optional should only have one items, but got {len(items)}: {items=!r}"
    return items[0] if items else None
# end def


def unpack_single[t](many: tuple[t]) -> MaybeTuple[t]:
    """Unpack a single-element tuple to its element."""
    if len(tuple) == 1:
        return tuple[0]
    # end if
    return tuple
# end def
