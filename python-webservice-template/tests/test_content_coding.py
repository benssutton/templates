import pytest

from core.content_coding import accepts_zstd, choose


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (None, None),
        ("", None),
        ("identity", None),
        ("br, deflate", None),
        ("zstd", "zstd"),
        ("gzip", "gzip"),
        ("gzip, zstd", "zstd"),
        ("zstd, gzip", "zstd"),
        ("zstd;q=1, gzip;q=1", "zstd"),
        ("zstd;q=0.5, gzip", "gzip"),
        ("ZSTD", "zstd"),
        ("zstd;Q=0.1", "zstd"),
        ("zstd ; q=0.2 , gzip;q=0.1", "zstd"),
        ("zstd;q=0", None),
        ("zstd;q=0, gzip", "gzip"),
        ("*", "zstd"),
        ("*;q=0.5, gzip", "gzip"),
        ("*, zstd;q=0", "gzip"),
        ("zstd;q=abc", None),
        ("zstd;q=nan", None),
        ("zstd;q=2", "zstd"),
        (" , zstd ,", "zstd"),
        ("zstd;q=0, zstd", "zstd"),
    ],
)
def test_choose(header: str | None, expected: str | None) -> None:
    assert choose(header) == expected


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (None, False),
        ("", False),
        ("identity", False),
        ("gzip", False),
        ("zstd;q=0", False),
        ("*, zstd;q=0", False),
        ("zstd", True),
        ("gzip, zstd;q=0.1", True),
        ("*", True),
    ],
)
def test_accepts_zstd(header: str | None, expected: bool) -> None:
    assert accepts_zstd(header) is expected
