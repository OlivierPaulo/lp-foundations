"""Tests for the pipeline module"""
import pandas as pd

from life_expectancy.pipeline import main
from . import FIXTURES_DIR


def test_main(pt_life_expectancy_expected):
    """Run the `main` function and compare the output to the expected output"""
    actual_data = main(
        file_name=f"{FIXTURES_DIR}/eu_life_expectancy_raw_fixture.tsv", regions="PT"
    ).reset_index(drop=True)
    expected_data = pt_life_expectancy_expected
    pd.testing.assert_frame_equal(actual_data, expected_data)
