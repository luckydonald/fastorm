# Verify the Property() emulation
from typing import ClassVar, AnyStr, Any

from fastorm.property import Property

class CC:
    doc_counter: ClassVar[int] = 0

    def getx(self):
        return self.__x
    def setx(self, value):
        self.__x = value
    def delx(self):
        del self.__x
    def docx(self):
        """
        This is a docstring for the 'docx' property.
        It can be used to test if the docstring is correctly set.
        """
        CC.doc_counter += 1
        return f"Doc call #{CC.doc_counter}"
    # end def

    x = Property(getx, setx, delx, "I'm the 'x' property.")
    no_getter = Property(None, setx, delx, "I'm the 'x' property.")
    no_setter = Property(getx, None, delx, "I'm the 'x' property.")
    no_deleter = Property(getx, setx, None, "I'm the 'x' property.")
    no_doc = Property(getx, setx, delx, None)
    counter_doc = Property(getx, setx, delx, None, docx)



# Now do it again but use the decorator style

class CCC:
    __x: Any

    @Property
    def x(self):
        return self.__x
    # end def

    @x.documenter
    def x(self):
        return f'Doc call {self.__x}'
    # end def

    @x.setter
    def x(self, value):
        self.__x = value
    # end def

    @x.deleter
    def x(self):
        del self.__x
    # end def
# end class


import unittest

class TestProperty(unittest.TestCase):
    def test_property_get_set_del(self):
        cc = CC()
        self.assertFalse(hasattr(cc, "x"))
        cc.x = 33
        self.assertEqual(cc.x, 33)
        del cc.x
        self.assertFalse(hasattr(cc, "x"))
        cc.x = 44  # Should not raise

    def test_property_decorator(self):
        ccc = CCC()
        self.assertFalse(hasattr(ccc, "x"))
        ccc.x = 333
        self.assertTrue(hasattr(ccc, "x"))
        self.assertEqual(ccc.x, 333)
        del ccc.x
        self.assertFalse(hasattr(ccc, "x"))
        ccc.x = 444
        self.assertEqual(ccc.x, 444)

    def test_property_decorator_docs(self):
        ccc = CCC()
        ccc.x = 123
        self.assertEqual(ccc.x.__doc__, 'Doc call #123')

    def test_no_getter(self):
        cc = CC()
        cc.x = 33
        with self.assertRaises(AttributeError) as cm:
            _ = cc.no_getter
        self.assertEqual(str(cm.exception), "property 'no_getter' of 'CC' object has no getter")

    def test_no_setter(self):
        cc = CC()
        with self.assertRaises(AttributeError) as cm:
            cc.no_setter = 33
        self.assertEqual(str(cm.exception), "property 'no_setter' of 'CC' object has no setter")

    def test_no_deleter(self):
        cc = CC()
        with self.assertRaises(AttributeError) as cm:
            del cc.no_deleter
        self.assertEqual(str(cm.exception), "property 'no_deleter' of 'CC' object has no deleter")

    def test_no_doc(self):
        self.assertIsNone(CC.no_doc.__doc__)

    def test_fdoc(self):
        self.assertEqual(CC.counter_doc.__doc__, 'Doc call #1')
        self.assertEqual(CC.counter_doc.__doc__, 'Doc call #2')

if __name__ == "__main__":
    unittest.main()