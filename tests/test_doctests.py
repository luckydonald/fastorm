import doctest
import fastorm
import unittest


def load_tests(loader, tests, ignore):
    tests.addTests(doctest.DocTestSuite(module=fastorm))
    tests.addTests(doctest.DocTestSuite(module=fastorm.query))
    return tests
# end def


if __name__ == '__main__':
    unittest.main()
# end if
