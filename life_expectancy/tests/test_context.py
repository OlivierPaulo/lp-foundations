"""Test of context functions"""

from unittest.mock import patch
import pandas as pd
from life_expectancy.context import Pipeline


@patch("life_expectancy.strategy.Default.save_data")
def test_executeStrategy(mock, pt_life_expectancy_expected):
    """Run the `main` function and compare the output to the expected output"""
    mock.side_effect = print("Mocking save data")
    actual_data = Pipeline().executeStrategy().reset_index(drop=True)
    expected_data = pt_life_expectancy_expected.reset_index(drop=True)
    pd.testing.assert_frame_equal(actual_data.reset_index(drop=True), expected_data)
