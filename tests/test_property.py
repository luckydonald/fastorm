# Verify the Property() emulation

from fastorm.property import Property

class CC:
    def getx(self):
        return self.__x
    def setx(self, value):
        self.__x = value
    def delx(self):
        del self.__x
    x = Property(getx, setx, delx, "I'm the 'x' property.")
    no_getter = Property(None, setx, delx, "I'm the 'x' property.")
    no_setter = Property(getx, None, delx, "I'm the 'x' property.")
    no_deleter = Property(getx, setx, None, "I'm the 'x' property.")
    no_doc = Property(getx, setx, delx, None)


# Now do it again but use the decorator style

class CCC:
    @Property
    def x(self):
        return self.__x
    @x.setter
    def x(self, value):
        self.__x = value
    @x.deleter
    def x(self):
        del self.__x

def test_property_via_docstring():
    """
    >>> cc = CC()
    >>> hasattr(cc, 'x')
    False
    >>> cc.x = 33
    >>> cc.x
    33
    >>> del cc.x
    >>> hasattr(cc, 'x')
    False

    >>> ccc = CCC()
    >>> hasattr(ccc, 'x')
    False
    >>> ccc.x = 333
    >>> ccc.x == 333
    True
    >>> del ccc.x
    >>> hasattr(ccc, 'x')
    False

    >>> cc = CC()
    >>> cc.x = 33
    >>> try:
    ...     cc.no_getter
    ... except AttributeError as e:
    ...     e.args[0]
    ...
    "property 'no_getter' of 'CC' object has no getter"

    >>> try:
    ...     cc.no_setter = 33
    ... except AttributeError as e:
    ...     e.args[0]
    ...
    "property 'no_setter' of 'CC' object has no setter"

    >>> try:
    ...     del cc.no_deleter
    ... except AttributeError as e:
    ...     e.args[0]
    ...
    "property 'no_deleter' of 'CC' object has no deleter"

    >>> CC.no_doc.__doc__ is None
    True
    """

if __name__ == "__main__":
    import doctest
    doctest.testmod(optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)