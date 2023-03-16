"""Tests for the cleaning module"""
from unittest.mock import patch
import pandas as pd
from life_expectancy.cleaning import load_data, clean_data, save_data, main
from . import OUTPUT_DIR, FIXTURES_DIR


def test_load_data():
    """Run unit test of function `load_data`"""
    pd.testing.assert_frame_equal(
        load_data(f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv"),
        pd.read_csv(f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv", sep="\t"),
    )


def test_clean_data():
    """Run unit test of function `clean_data`"""
    pd.testing.assert_frame_equal(
        clean_data(
            pd.read_csv(f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv", sep="\t")
        ).reset_index(drop=True),
        pd.read_csv(FIXTURES_DIR / "pt_life_expectancy_expected.csv"),
    )


def test_save_data(pt_life_expectancy_expected):
    """Run unit test of function `save_data`"""
    with patch.object(pt_life_expectancy_expected, "to_csv") as to_csv_mock:
        save_data(pt_life_expectancy_expected)
        to_csv_mock.assert_called_with(
            OUTPUT_DIR.joinpath("pt_life_expectancy.csv"), index=False
        )


def test_main(pt_life_expectancy_expected):
    """Run the `main` function and compare the output to the expected output"""
    pd.testing.assert_frame_equal(
        main(
            file_name=f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv", regions="PT"
        ).reset_index(drop=True),
        pt_life_expectancy_expected,
    )
