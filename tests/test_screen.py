import unittest
import betterbloomberg as bb

class TestEQS(unittest.TestCase):
    request_type = "EQS"

    def setUp(self) -> None:
        self.screen_name = "Core Capital Ratios"
        self.screen_type = "GLOBAL"
        self.screen_group = "General"

    def test_run(self):
        data = bb.EQS(
            self.screen_name,
            self.screen_type,
            self.screen_group
        ).data
        self.assertFalse(data.empty)

