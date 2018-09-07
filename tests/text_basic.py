import unittest
from betterbloomberg import reference_data



class TestReferenceDataMethods(unittest.TestCase):
    pass

# I'm not sure I want to do this
suite = unitTest.TestLoader().loadFromTestCase(TestReferenceDataMethods)
unittest.TextTestRunner(verbosity=2).run(suite)
# if __name__ == '__main__':
#     unittest.main()