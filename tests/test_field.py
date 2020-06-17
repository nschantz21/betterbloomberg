import unittest
import betterbloomberg as bb


class TestFieldInfo(unittest.TestCase):

    def setUp(self) -> None:
        self.field_id = ["BEST_EPS", ]

    def test_simple(self) -> None:
        field_info = bb.FieldInfo(self.field_id, docs=True).data
        self.assertEqual(field_info["description"].iloc[0], "BEst EPS")


class TestFieldSearch(unittest.TestCase):

    def setUp(self) -> None:
        self.query = "EPS"

    def test_simple(self) -> None:
        search = bb.FieldSearch(self.query).data
        # I don't know how to best test the output
        self.assertFalse(search.empty)
