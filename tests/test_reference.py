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

    def test_security_error(self):
        with self.assertRaises(Exception) as ex:
            bb.ReferenceDataRequest(
                "bad sec",
                self.field
            )
        self.assertEqual(ex.exception.args[-1][-1], "INVALID_SECURITY")

    def test_not_all_bad(self):
        data = bb.ReferenceDataRequest(
            [
                "AAPL US Equity",
                "bad sec"
            ],
            self.field,
            ignore_sec_error=True
        ).data
        self.assertFalse(data.empty)


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

    def test_bad_sec(self):
        with self.assertRaises(Exception) as ex:
            bb.HistoricalDataRequest(
                "bad sec",
                self.field,
                self.start_date,
                self.end_date
            )
        self.assertEqual(ex.exception.args[-1][-1], "INVALID_SECURITY")

    def test_one_bad_sec(self):
        data = bb.HistoricalDataRequest(
            self.ticker + ["bad sec", ],
            self.field,
            self.start_date,
            self.end_date,
            ignore_sec_error=True
        ).data
        self.assertFalse(data.empty)

