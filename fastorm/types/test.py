from pydantic import BaseModel

class UserBase(BaseModel):
    username: str
    email: str

class ProfileBase(BaseModel):
    bio: str
    image: str


class UserProfile(UserBase, ProfileBase):
    joined_date: str


from typing import TypedDict

class Person(TypedDict):
    name: str
    age: int

class Address(TypedDict):
    street: str
    city: str

class PersonAddress(Person, Address, total=False):
    pass
