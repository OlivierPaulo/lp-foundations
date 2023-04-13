"""Tests for the pipeline module"""
from unittest.mock import patch
import pandas as pd

from life_expectancy.data import DataIOLoadTSV, DataIOLoadJSON, DataIO
from . import OUTPUT_DIR, FIXTURES_DIR


def test_load_data_tsv(eu_life_expectancy_tsv_expected):
    """Run unit test of function `load_data`"""
    actual_data = DataIOLoadTSV().load_data(
        f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv"
    )
    print(actual_data)
    expected_data = eu_life_expectancy_tsv_expected
    pd.testing.assert_frame_equal(actual_data, expected_data)


def test_load_data_json(eu_life_expectancy_json_expected):
    """Run unit test of function `load_data`"""
    actual_data = DataIOLoadJSON().load_data(
        f"{FIXTURES_DIR}/eurostat_life_raw_fixture.json"
    )
    expected_data = eu_life_expectancy_json_expected
    pd.testing.assert_frame_equal(actual_data, expected_data)


def test_save_data(pt_life_expectancy_expected):
    """Run unit test of function `save_data`"""
    expected_data = OUTPUT_DIR.joinpath("pt_life_expectancy.csv")
    with patch.object(pt_life_expectancy_expected, "to_csv") as to_csv_mock:
        to_csv_mock.side_effect = print("Mocking saving data...")
        DataIO().save_data(pt_life_expectancy_expected)
        to_csv_mock.assert_called_with(expected_data, index=False)
