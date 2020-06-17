import unittest
import betterbloomberg as bb


class TestGovSearch(unittest.TestCase):

    def setUp(self) -> None:
        self.query = "T"

    def test_run(self):
        data = bb.GovernmentSearch(
            self.query
        ).data
        self.assertFalse(data.empty)


class TestSecSearch(unittest.TestCase):

    def setUp(self) -> None:
        self.query = "AAPL"

    def test_run(self):
        data = bb.SecuritySearch(
            self.query
        ).data
        self.assertFalse(data.empty)


class TestCurveSearch(unittest.TestCase):

    def setUp(self) -> None:
        self.query = "Treasury"

    def test_run(self):
        data = bb.CurveSearch(
            self.query
        ).data
        self.assertFalse(data.empty)
