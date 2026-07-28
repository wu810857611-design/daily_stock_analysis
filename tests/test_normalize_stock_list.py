import unittest

from scripts.normalize_stock_list import canonical_symbol, normalize_stock_list, symbol_market


class NormalizeStockListTests(unittest.TestCase):
    def test_hong_kong_aliases_become_supported_prefix_format(self):
        self.assertEqual(canonical_symbol("HK.00981"), "HK00981")
        self.assertEqual(canonical_symbol("00981.HK"), "HK00981")
        self.assertEqual(canonical_symbol("hk6181"), "HK06181")

    def test_normalizes_common_separators_and_deduplicates(self):
        self.assertEqual(
            normalize_stock_list("HK.00981，00981.HK 300408;300408"),
            ["HK00981", "300408"],
        )

    def test_filters_market_batches(self):
        stocks = "HK.00981,300408,HK06181,688333,AAPL"
        self.assertEqual(normalize_stock_list(stocks, "hk"), ["HK00981", "HK06181"])
        self.assertEqual(normalize_stock_list(stocks, "cn"), ["300408", "688333"])
        self.assertEqual(normalize_stock_list(stocks, "other"), ["AAPL"])
        self.assertEqual(symbol_market("HK06166"), "hk")


if __name__ == "__main__":
    unittest.main()
