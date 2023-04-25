"""Test of context functions"""

from unittest.mock import patch
import pandas as pd
from life_expectancy.pipelines import Pipeline, FileTSV, FileJSON, Country
from . import FIXTURES_DIR, OUTPUT_DIR

tsv_pipe = FileTSV()
json_pipe = FileJSON()


@patch("life_expectancy.pipelines.FileTSV.save_data")
def test_execute_strategy(mock, pt_life_expectancy_expected):
    """Run the `execute_strategy` function and compare the output to the expected output"""
    mock.side_effect = print("Mocking save data")
    actual_data = (
        Pipeline(
            source_file=f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv",
            countries=[Country.PT],
        )
        .execute_strategy()
        .reset_index(drop=True)
    )
    expected_data = pt_life_expectancy_expected.reset_index(drop=True)
    pd.testing.assert_frame_equal(actual_data.reset_index(drop=True), expected_data)


@patch("life_expectancy.pipelines.Strategy.save_data")
def test_save_data(pt_life_expectancy_expected, pt_life_expectancy_json_expected):
    """Run unit test of method `save_data`"""
    expected_from_tsv_data = OUTPUT_DIR.joinpath("pt_life_expectancy.csv")
    expected_from_json_data = OUTPUT_DIR.joinpath(
        "pt_life_expectancy_json_expected.csv"
    )
    with patch.object(pt_life_expectancy_expected, "to_csv") as to_csv_mock:
        to_csv_mock.side_effect = print("Mocking TSV -> CSV saving data...")
        tsv_pipe.save_data(pt_life_expectancy_expected, [Country.PT])
        to_csv_mock.assert_called_with(expected_from_tsv_data, index=False)
        to_csv_mock.side_effect = print("Mocking JSON -> CSV saving data...")
        json_pipe.save_data(pt_life_expectancy_json_expected, [Country.PT])
        to_csv_mock.assert_called_with(expected_from_json_data, index=False)


@patch("life_expectancy.pipelines.Strategy.save_data")
def test_execute(mock, pt_life_expectancy_expected):
    """Run the `execute` of default pipe function and compare the output to the expected output"""
    mock.side_effect = print("Mocking save data")
    actual_data = tsv_pipe.execute(
        source_file=f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv",
        countries=[Country.PT],
    ).reset_index(drop=True)
    expected_data = pt_life_expectancy_expected.reset_index(drop=True)
    pd.testing.assert_frame_equal(actual_data.reset_index(drop=True), expected_data)


def test_load_data_tsv(eu_life_expectancy_tsv_expected):
    """Run unit test of function `load_data` for TSV source file"""
    actual_data = tsv_pipe.load_data(
        source_file=f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv"
    )
    expected_data = eu_life_expectancy_tsv_expected
    pd.testing.assert_frame_equal(actual_data, expected_data)


def test_clean_data_tsv(pt_life_expectancy_expected, eu_life_expectancy_tsv_expected):
    """Run unit test of function `clean_data` for TSV source file"""
    actual_data = tsv_pipe.clean_data(
        eu_life_expectancy_tsv_expected, countries=[Country.PT]
    ).reset_index(drop=True)
    expected_data = pt_life_expectancy_expected
    pd.testing.assert_frame_equal(actual_data, expected_data)


def test_load_data_json(eu_life_expectancy_load_json_expected):
    """Run unit test of function `load_data` for JSON source file"""
    actual_data = FileJSON().load_data(
        f"{FIXTURES_DIR}/eurostat_life_expect_fixture.json"
    )
    expected_data = eu_life_expectancy_load_json_expected
    pd.testing.assert_frame_equal(actual_data, expected_data)


def test_clean_data_json(
    eu_life_expectancy_load_json_expected, pt_life_expectancy_json_expected
):
    """Run unit test of function `clean_data` for JSON source file"""
    actual_data = json_pipe.clean_data(
        eu_life_expectancy_load_json_expected, countries=[Country.PT]
    ).reset_index(drop=True)
    expected_data = pt_life_expectancy_json_expected
    pd.testing.assert_frame_equal(actual_data, expected_data)


def test_possible_countries():
    """Function that test the possible countries"""
    actual_possible_contries = Country.possible_countries()
    expected_possible_countries = [
        "AL",
        "AM",
        "AT",
        "AZ",
        "BE",
        "BG",
        "BY",
        "CH",
        "CY",
        "CZ",
        "DE",
        "DK",
        "EE",
        "EL",
        "ES",
        "FI",
        "FR",
        "FX",
        "GE",
        "HR",
        "HU",
        "IE",
        "IS",
        "IT",
        "LI",
        "LT",
        "LU",
        "LV",
        "MD",
        "ME",
        "MK",
        "MT",
        "NL",
        "NO",
        "PL",
        "PT",
        "RO",
        "RS",
        "RU",
        "SE",
        "SI",
        "SK",
        "SM",
        "TR",
        "UA",
        "UK",
        "XK",
    ]
    assert actual_possible_contries == expected_possible_countries
