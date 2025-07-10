import doctest
from fastorm import query

# Create a test suite for the doctests
def load_tests(loader, tests, ignore):
    suite = doctest.DocTestSuite(module=query)
    tests.addTests(suite)
    return tests