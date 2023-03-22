"""Tests for the pipeline module"""
from unittest.mock import patch
import pandas as pd

from life_expectancy.data import load_data, save_data
from . import OUTPUT_DIR, FIXTURES_DIR


def test_load_data():
    """Run unit test of function `load_data`"""
    actual_data = load_data(f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv")
    expected_data = pd.read_csv(
        f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv", sep="\t"
    )
    pd.testing.assert_frame_equal(actual_data, expected_data)


def test_save_data(pt_life_expectancy_expected):
    """Run unit test of function `save_data`"""
    with patch.object(pt_life_expectancy_expected, "to_csv") as to_csv_mock:
        save_data(pt_life_expectancy_expected)
        to_csv_mock.assert_called_with(
            OUTPUT_DIR.joinpath("pt_life_expectancy.csv"), index=False
        )
