"""Tests for the pipeline module"""
from unittest.mock import patch
import pandas as pd

from life_expectancy.data import load_data, save_data
from . import OUTPUT_DIR, FIXTURES_DIR


def test_load_data():
    """Run unit test of function `load_data`"""
    pd.testing.assert_frame_equal(
        load_data(f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv"),
        pd.read_csv(f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv", sep="\t"),
    )


def test_save_data(pt_life_expectancy_expected):
    """Run unit test of function `save_data`"""
    with patch.object(pt_life_expectancy_expected, "to_csv") as to_csv_mock:
        save_data(pt_life_expectancy_expected)
        to_csv_mock.assert_called_with(
            OUTPUT_DIR.joinpath("pt_life_expectancy.csv"), index=False
        )
