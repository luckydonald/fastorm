from typing import TypeVar, Generic, Type, Any, get_type_hints, get_origin, get_args
from abc import ABC, abstractmethod

# Type variable for the primary key type
PKType = TypeVar('PKType')


class PK(Generic[PKType]):
    """Primary Key annotation type."""

    def __init__(self, value: PKType | None = None):
        self.value = value

    def __class_getitem__(cls, item):
        """Support for PK[int], PK[str], etc."""
        return super().__class_getitem__(item)


class OrmModelMeta(type):
    """Metaclass that extracts primary key type information."""

    def __new__(mcs, name, bases, namespace, **kwargs):
        cls = super().__new__(mcs, name, bases, namespace)

        # Get type hints for this class
        hints = get_type_hints(cls)

        # Find the primary key field and its type
        pk_field = None
        pk_type = None

        for field_name, field_type in hints.items():
            if get_origin(field_type) is PK:
                pk_field = field_name
                pk_type = get_args(field_type)[0]
                break

        # Store the primary key information on the class
        cls._pk_field = pk_field
        cls._pk_type = pk_type

        return cls


class OrmModel(metaclass=OrmModelMeta):
    """Base model class with a primary key."""

    _pk_field: str | None = None
    _pk_type: Type[Any] | None = None

    def __init__(self, **kwargs):
        """Initialize the model with field values."""
        # Set attributes from kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)

        # Initialize primary key field if not provided
        if self._pk_field and not hasattr(self, self._pk_field):
            setattr(self, self._pk_field, None)

    @property
    def pk(self):
        """Returns the primary key of the model."""
        if self._pk_field is None:
            raise ValueError("No primary key field defined")
        return getattr(self, self._pk_field)

    @pk.setter
    def pk(self, value):
        """Sets the primary key of the model."""
        if self._pk_field is None:
            raise ValueError("No primary key field defined")
        setattr(self, self._pk_field, value)

    def __repr__(self):
        """String representation of the model."""
        class_name = self.__class__.__name__
        if self._pk_field:
            pk_value = getattr(self, self._pk_field, None)
            return f"{class_name}({self._pk_field}={pk_value!r})"
        return f"{class_name}()"


# Example usage
class ExampleTableWithAutoincrement(OrmModel):
    id: PK[int]
    name: str
    description: str

    def __init__(self, id: int | None = None, name: str = "", description: str = ""):
        super().__init__(id=id, name=name, description=description)


class UserModel(OrmModel):
    user_id: PK[str]
    username: str
    email: str

    def __init__(self, user_id: str | None = None, username: str = "", email: str = ""):
        super().__init__(user_id=user_id, username=username, email=email)


# Demo usage
if __name__ == "__main__":
    # Create instances
    example = ExampleTableWithAutoincrement(id=1, name="Test", description="A test record")
    user = UserModel(user_id="user123", username="john_doe", email="john@example.com")

    # Access primary keys - these will be properly typed
    print(f"Example PK: {example.pk}")  # Type: int
    print(f"User PK: {user.pk}")  # Type: str

    # Modify primary keys
    example.pk = 42
    user.pk = "user456"

    print(f"Updated Example PK: {example.pk}")
    print(f"Updated User PK: {user.pk}")

    # Show class information
    print(f"Example PK field: {ExampleTableWithAutoincrement._pk_field}")
    print(f"Example PK type: {ExampleTableWithAutoincrement._pk_type}")
    print(f"User PK field: {UserModel._pk_field}")
    print(f"User PK type: {UserModel._pk_type}")