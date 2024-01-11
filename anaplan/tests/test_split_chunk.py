import unittest

from anaplan.splitBytesIntoChunks import splitBytesIntoChuncks


class Test_split_bytes_into_chunks(unittest.TestCase):
    def test_split_bytes(self):
        data = bytes(8)
        result = []
        for chunk in splitBytesIntoChuncks(data, 4):
            result.append(chunk)
        assert len(result) == 2
