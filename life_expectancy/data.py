"""Python 3.11.2"""

from pathlib import Path
import pandas as pd

DIR_PATH = Path(__file__).parent


def load_data(
    file_name: str = "data/eu_life_expectancy_raw.tsv",
) -> pd.DataFrame:
    """Load data from file and Return a Pandas DataFrame"""
    return pd.read_csv(DIR_PATH.joinpath(file_name), sep="\t")


def save_data(data_frame: pd.DataFrame, country: str = "pt") -> None:
    """Function that saves the data into a local CSV file"""
    data_frame.to_csv(
        DIR_PATH.joinpath(f"data/{country.lower()}_life_expectancy.csv"), index=False
    )
