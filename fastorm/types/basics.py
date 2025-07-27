from datetime import datetime
from typing import TypeVar, Any, Annotated, Union
from uuid import UUID

AType = TypeVar("AType")

type Namespace = dict[str, object]  # Class attributes/methods

AutoSupporting = int | UUID | datetime
AutoSupportingType = TypeVar("AutoSupportingType", bound=AutoSupporting)

type MaybeTuple[T] = T | tuple[T, ...]
type MaybeTypeType[T] = MaybeTuple[type[T]]

AnnotationType = type[Any] | None
AnnotatedType = type(Annotated[str, "some metadata"])

PrimaryKeyDataType = TypeVar("PrimaryKeyDataType", bound=type)
PrimaryKeyDataTypeArg = Union[PrimaryKeyDataType, tuple[PrimaryKeyDataType, ...]]
PrimaryKeyDataTypeArgType = type[PrimaryKeyDataType] | tuple[type[PrimaryKeyDataType], ...]