"""Unit tests for the specification (via the reference parser).

Run:  python -m unittest discover -s tests -v
"""
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from reference_oracle import parse_line  # noqa: E402

BASE = '%s - - [01/Jul/1995:00:00:01 -0400] %s'


class ParserSpec(unittest.TestCase):
    def test_normal_line(self):
        r = parse_line('199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245')
        self.assertEqual(r["host"], "199.72.81.55")
        self.assertEqual(r["timestamp"], "1995-07-01 00:00:01")
        self.assertEqual((r["log_date"], r["log_hour"]), ("1995-07-01", 0))
        self.assertEqual((r["http_method"], r["resource_path"], r["protocol_version"]),
                         ("GET", "/history/apollo/", "HTTP/1.0"))
        self.assertEqual((r["status_code"], r["bytes_transferred"]), (200, 6245))

    def test_dash_bytes_become_zero(self):
        self.assertEqual(parse_line(BASE % ("h", '"GET /a HTTP/1.0" 304 -'))["bytes_transferred"], 0)

    def test_absent_bytes_become_zero(self):
        for suffix in ("", "   "):
            r = parse_line(BASE % ("h", '"GET /a HTTP/1.0" 200' + suffix))
            self.assertEqual((r["status_code"], r["bytes_transferred"]), (200, 0))

    def test_non_numeric_bytes_become_zero(self):
        self.assertEqual(parse_line(BASE % ("h", '"GET /a HTTP/1.0" 200 abc'))["bytes_transferred"], 0)

    def test_missing_protocol_is_null_but_valid(self):
        r = parse_line(BASE % ("h", '"GET /a" 200 1'))
        self.assertEqual((r["http_method"], r["resource_path"], r["protocol_version"]), ("GET", "/a", None))

    def test_one_token_and_empty_request(self):
        r = parse_line(BASE % ("h", '"GET" 400 -'))
        self.assertEqual((r["http_method"], r["resource_path"]), ("GET", None))
        r = parse_line(BASE % ("h", '"" 400 -'))
        self.assertEqual((r["http_method"], r["resource_path"], r["protocol_version"]), (None, None, None))

    def test_extra_tokens_ignored(self):
        r = parse_line(BASE % ("h", '"GET /a b HTTP/1.0" 200 1'))
        self.assertEqual((r["resource_path"], r["protocol_version"]), ("/a", "b"))

    def test_quotes_inside_request(self):
        self.assertIsNotNone(parse_line(BASE % ("h", '"GET /x"y HTTP/1.0" 404 -')))

    def test_malformed_lines(self):
        for line in ("alyssa.p",
                     BASE % ("h", '"GET /a HTTP/1.0" 2000 1'),
                     '%s - - [01/Foo/1995:00:00:01 -0400] "GET / HTTP/1.0" 200 1' % "h",
                     '%s - - [01/Jul/1995:24:00:00 -0400] "GET / HTTP/1.0" 200 1' % "h",
                     '%s - - [32/Jul/1995:01:00:00 -0400] "GET / HTTP/1.0" 200 1' % "h"):
            self.assertIsNone(parse_line(line), line)


if __name__ == "__main__":
    unittest.main()
