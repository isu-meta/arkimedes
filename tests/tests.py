from pathlib import Path

import pytest

import arkimedes

# EZID tests


def test_build_anvl():
    test_dict = {"a": "b", "c": "d", "e": "f"}
    expected = "a: b\nc: d\ne: f"
    assert arkimedes.ezid.build_anvl(test_dict) == expected


def test_anvl_to_dict():
    test_string = ":: ark/example\na: b\nc: d\ne: https://example.edu"
    expected = {
        "ark": "ark/example",
        "a": "b",
        "c": "d",
        "e": "https://example.edu",
    }
    assert arkimedes.ezid.anvl_to_dict(test_string) == expected


def test_anvls_to_dict():
    test_string = ":: ark/example\na: b\nc: d\ne: https://example.edu\n\n:: ark/ex\na: b\nc: d\ne: https://example.org\n\n"
    expected = [
        {
            "ark": "ark/example",
            "a": "b",
            "c": "d",
            "e": "https://example.edu",
        },
        {
            "ark": "ark/ex",
            "a": "b",
            "c": "d",
            "e": "https://example.org",
        },
    ]
    assert arkimedes.ezid.anvls_to_dict(test_string) == expected


def test_load_anvls_from_tsv():
    base_path = Path(__file__).parent.resolve()
    dict_expected = [
        {"head1": "foo", "head2": "bar", "head3": "baz"},
        {"head1": "big", "head2": "bad", "head3": "wolf"},
    ]

    str_expected = [
        "head1: foo\nhead2: bar\nhead3: baz",
        "head1: big\nhead2: bad\nhead3: wolf",
    ]

    assert (
        list(
            arkimedes.ezid.load_anvl_as_dict_from_tsv(
                base_path / "data/test.csv"
            )
        )
        == dict_expected
    )
    assert (
        list(
            arkimedes.ezid.load_anvl_as_dict_from_tsv(
                base_path / "data/test.tsv"
            )
        )
        == dict_expected
    )

    assert (
        list(
            arkimedes.ezid.load_anvl_as_str_from_tsv(
                base_path / "data/test.csv"
            )
        )
        == str_expected
    )
    assert (
        list(
            arkimedes.ezid.load_anvl_as_str_from_tsv(
                base_path / "data/test.tsv"
            )
        )
        == str_expected
    )
