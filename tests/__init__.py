import unittest


class InitializationTests(unittest.TestCase):

    def test_initialization(self):
        self.assertEqual(2+2, 4)

    def test_import(self):
        try:
            import domsanalysis
        except:
            raise


