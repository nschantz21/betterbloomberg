import unittest
import betterbloomberg as bb


class TestStudy(unittest.TestCase):

    def setUp(self):
        self.security = "IBM US Equity"
        self.study = "dmi"
        self.start = "20200101"
        self.end = "20200201"

    def test_run(self):
        data = bb.Study(
            self.security,
            self.study,
            self.start,
            self.end,
            priceSourceLow="PX_LOW",
            priceSourceClose="PX_LAST",
            priceSourceHigh="PX_HIGH"
        ).data
        self.assertFalse(data.empty)

    def test_bollinger(self):
        data = bb.Study(
            self.security,
            "boll",
            self.start,
            self.end,
            priceSourceClose="PX_LAST"
        ).data
        self.assertFalse(data.empty)
