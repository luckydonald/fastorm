from typing import TypeVar, Any, Annotated
from uuid import UUID

AType = TypeVar("AType")

AutoSupporting = int | UUID
AutoSupportingType = TypeVar("AutoSupportingType", bound=AutoSupporting)

type MaybeTuple[T] = T | tuple[T, ...]
type MaybeTypeType[T] = MaybeTuple[type[T]]

AnnotationType = type[Any] | None
AnnotatedType = type(Annotated[str, "some metadata"])

PrimaryKeyDataType = TypeVar("PrimaryKeyDataType")
PrimaryKeyDataTypeArg = PrimaryKeyDataType | tuple[PrimaryKeyDataType, ...]
PrimaryKeyDataTypeArgType = type[PrimaryKeyDataType] | tuple[type[PrimaryKeyDataType], ...]