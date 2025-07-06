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
        self.assertEqual(ccc.x, 333)
        del ccc.x
        self.assertFalse(hasattr(ccc, "x"))

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

if __name__ == "__main__":
    unittest.main()