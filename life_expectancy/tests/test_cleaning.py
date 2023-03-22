"""Tests for the cleaning module"""
import pandas as pd
from life_expectancy.cleaning import clean_data
from . import FIXTURES_DIR


def test_clean_data(pt_life_expectancy_expected):
    """Run unit test of function `clean_data`"""
    actual_data = clean_data(
        pd.read_csv(f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv", sep="\t")
    ).reset_index(drop=True)
    expected_data = pt_life_expectancy_expected
    pd.testing.assert_frame_equal(actual_data, expected_data)
