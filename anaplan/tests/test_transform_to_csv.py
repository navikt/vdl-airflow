import unittest

from anaplan.get_data import transform_to_csv


class Test_transform_to_csv(unittest.TestCase):
    def test_unicode_symbols(self):
        column_names = ["Kake"]
        data = [("æøå",)]
        transform_to_csv(data, column_names).decode("UTF-8")

    def test_column_names(self):
        column_names = ["Kake"]
        data = [("æøå",)]
        result = transform_to_csv(data, column_names).decode("UTF-8")
        expected = "Kake"
        assert result.split()[0] == expected
