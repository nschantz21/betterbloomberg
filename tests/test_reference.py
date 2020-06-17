import unittest
import betterbloomberg as bb


class TestReferenceRequest(unittest.TestCase):

    def setUp(self) -> None:
        self.ticker = "AAPL US Equity"
        self.field = "PARSEKYABLE_DES"

    def test_run(self):
        data = bb.ReferenceDataRequest(
            self.ticker,
            self.field
        ).data
        self.assertEqual(
            self.ticker,
            data.values[0][0]
        )

class TestHistoricalRequest(unittest.TestCase):

    def setUp(self) -> None:
        self.ticker = ["AAPL US Equity",]
        self.field = ["PX_LAST",]
        self.start_date = "19990104"
        self.end_date = "19990201"

    def test_run(self):
        data = bb.HistoricalDataRequest(
            self.ticker,
            self.field,
            self.start_date,
            self.end_date
        ).data

        self.assertAlmostEqual(
            data.iloc[0, 0],
            1.473  # price on 1999-01-04
        )


