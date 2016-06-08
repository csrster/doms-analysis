from unittest import TestCase
from domsanalysis import config


class TestSomething(TestCase):

    def test_init(self):
        pass

    def test_initialization(self):
        self.assertEqual(2+2, 4)

    def test_import(self):
        try:
            import domsanalysis
        except:
            raise

    def testConfigFail(self):
        with self.assertRaises(SystemExit):
            config.parseOpts(('foobar', '-h'))

