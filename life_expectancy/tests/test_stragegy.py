"""Test of strategy functions"""

from unittest.mock import patch
import pandas as pd
from life_expectancy.strategy import FileJSON, FileTSV, Default, Country
from . import FIXTURES_DIR, OUTPUT_DIR


tsv_pipe = FileTSV()
json_pipe = FileJSON()
default_pipe = Default()


def test_load_data_tsv(eu_life_expectancy_tsv_expected):
    """Run unit test of function `load_data` for TSV source file"""
    actual_data = tsv_pipe.load_data(
        f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv"
    )
    expected_data = eu_life_expectancy_tsv_expected
    pd.testing.assert_frame_equal(actual_data, expected_data)


def test_clean_data_tsv(pt_life_expectancy_expected, eu_life_expectancy_tsv_expected):
    """Run unit test of function `clean_data` for TSV source file"""
    actual_data = tsv_pipe.clean_data(eu_life_expectancy_tsv_expected).reset_index(
        drop=True
    )
    expected_data = pt_life_expectancy_expected
    pd.testing.assert_frame_equal(actual_data, expected_data)


def test_save_data_tsv(pt_life_expectancy_expected):
    """Run unit test of function `save_data`"""
    expected_data = OUTPUT_DIR.joinpath("pt_life_expectancy.csv")
    with patch.object(pt_life_expectancy_expected, "to_csv") as to_csv_mock:
        to_csv_mock.side_effect = print("Mocking saving data...")
        tsv_pipe.save_data(pt_life_expectancy_expected)
        to_csv_mock.assert_called_with(expected_data, index=False)


@patch("life_expectancy.strategy.FileTSV.save_data")
def test_execute_tsv(mock, pt_life_expectancy_expected):
    """Run the `main` function and compare the output to the expected output"""
    mock.side_effect = print("Mocking save data")
    actual_data = tsv_pipe.execute(
        f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv"
    ).reset_index(drop=True)
    expected_data = pt_life_expectancy_expected.reset_index(drop=True)
    pd.testing.assert_frame_equal(actual_data.reset_index(drop=True), expected_data)


def test_load_data_json(eu_life_expectancy_load_json_expected):
    """Run unit test of function `load_data` for JSON source file"""
    actual_data = FileJSON().load_data(
        f"{FIXTURES_DIR}/eurostat_life_expect_fixture.json"
    )
    expected_data = eu_life_expectancy_load_json_expected
    pd.testing.assert_frame_equal(actual_data, expected_data)


### Need to replace empty and None by NaN values in read_json
def test_clean_data_json(
    eu_life_expectancy_load_json_expected, pt_life_expectancy_json_expected
):
    """Run unit test of function `clean_data` for JSON source file"""
    actual_data = json_pipe.clean_data(
        eu_life_expectancy_load_json_expected
    ).reset_index(drop=True)
    expected_data = pt_life_expectancy_json_expected
    pd.testing.assert_frame_equal(actual_data, expected_data)


def test_save_data_json(pt_life_expectancy_json_expected):
    """Run unit test of function `save_data` for JSON source file"""
    expected_data = OUTPUT_DIR.joinpath("pt_life_expectancy.csv")
    with patch.object(pt_life_expectancy_json_expected, "to_csv") as to_csv_mock:
        to_csv_mock.side_effect = print("Mocking saving data...")
        tsv_pipe.save_data(pt_life_expectancy_json_expected)
        to_csv_mock.assert_called_with(expected_data, index=False)


@patch("life_expectancy.strategy.FileJSON.save_data")
def test_execute_json(mock, pt_life_expectancy_json_expected):
    """Run the `main` function and compare the output to the expected output"""
    mock.side_effect = print("Mocking save data")
    actual_data = json_pipe.execute(
        f"{FIXTURES_DIR}/eurostat_life_expect_fixture.json"
    ).reset_index(drop=True)
    expected_data = pt_life_expectancy_json_expected.reset_index(drop=True)
    pd.testing.assert_frame_equal(actual_data.reset_index(drop=True), expected_data)


def test_load_data_default(eu_life_expectancy_tsv_expected):
    """Run unit test of function `load_data` for TSV source file"""
    actual_data = default_pipe.load_data(
        f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv"
    )
    expected_data = eu_life_expectancy_tsv_expected
    pd.testing.assert_frame_equal(actual_data, expected_data)


def test_clean_data_default(
    pt_life_expectancy_expected, eu_life_expectancy_tsv_expected
):
    """Run unit test of function `clean_data` for TSV source file"""
    actual_data = default_pipe.clean_data(eu_life_expectancy_tsv_expected).reset_index(
        drop=True
    )
    expected_data = pt_life_expectancy_expected
    pd.testing.assert_frame_equal(actual_data, expected_data)


def test_save_data_default(pt_life_expectancy_expected):
    """Run unit test of function `save_data`"""
    expected_data = OUTPUT_DIR.joinpath("pt_life_expectancy.csv")
    with patch.object(pt_life_expectancy_expected, "to_csv") as to_csv_mock:
        to_csv_mock.side_effect = print("Mocking saving data...")
        default_pipe.save_data(pt_life_expectancy_expected)
        to_csv_mock.assert_called_with(expected_data, index=False)


@patch("life_expectancy.strategy.Default.save_data")
def test_execute_default(mock, pt_life_expectancy_expected):
    """Run the `execute` of default pipe function and compare the output to the expected output"""
    mock.side_effect = print("Mocking save data")
    actual_data = default_pipe.execute(
        f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv"
    ).reset_index(drop=True)
    expected_data = pt_life_expectancy_expected.reset_index(drop=True)
    pd.testing.assert_frame_equal(actual_data.reset_index(drop=True), expected_data)


def test_possible_countries():
    """Function that test the possible countries"""
    actual_possible_contries = Country.possible_countries()
    expected_possible_countries = ["PT", "FR", "NL"]
    assert actual_possible_contries == expected_possible_countries
