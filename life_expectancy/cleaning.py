"""Python 3.11.2"""

from pathlib import Path
from typing import List, Dict
import argparse
import pandas as pd

DIR_PATH = Path(__file__).parent


def load_data(
    file_name: str = "data/eu_life_expectancy_raw.tsv",
) -> pd.DataFrame:
    """Load data from file and Return a Pandas DataFrame"""
    return pd.read_csv(DIR_PATH.joinpath(file_name), sep="\t")


def _apply_unpivot(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Return Dataframe with the unpivots dates and desired columns"""
    id_vars = data_frame.columns[0]
    col_names = ["unit", "sex", "age", "region", "year", "value"]
    unpivot_df = pd.melt(frame=data_frame, id_vars=id_vars)
    unpivot_df[id_vars.split(",")] = unpivot_df[id_vars].str.split(",", expand=True)
    unpivot_df[col_names] = pd.concat(
        [unpivot_df[id_vars.split(",")], unpivot_df[["variable", "value"]]], axis=1
    )
    return unpivot_df[col_names]


def _apply_data_types(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Ensure data types defined by type_rules, Clean and Extract data using Regex
    Remove NaNs for requested cols"""
    types_rules: Dict[str, object] = {"year": int, "value": float}
    cols_to_delete: List[str] = ["value"]
    for column, data_type in types_rules.items():
        data_frame[column] = pd.to_numeric(
            data_frame[column]
            .str.extractall(r"(\d+.\d+)")
            .astype(data_type)
            .unstack()
            .max(axis=1),
            errors="coerce",
        )
    return data_frame.dropna(subset=cols_to_delete)


def clean_data(data_frame: pd.DataFrame, country: str = "PT") -> pd.DataFrame:
    """Main function to Clean Data and Filter Region"""
    clean_df = data_frame.pipe(_apply_unpivot).pipe(_apply_data_types)
    return clean_df[clean_df.region.str.upper() == country.upper()]


def save_data(data_frame: pd.DataFrame, country: str = "pt") -> None:
    """Function that saves the data into a local CSV file"""
    data_frame.to_csv(
        DIR_PATH.joinpath(f"data/{country.lower()}_life_expectancy.csv"), index=False
    )


# def main(country: str = "PT", file_name: str ="") -> None:
def main(*args, **kwargs) -> None:
    """Main Function which call functions of the data pipeline"""
    if args:
        raw_df = load_data()
        for country in args:
            clean_df = clean_data(raw_df, country)
            save_data(clean_df, country)
    if kwargs:
        raw_df = load_data(kwargs["file_name"])
        for country in kwargs["regions"].split(","):
            clean_df = clean_data(raw_df, country)
            save_data(clean_df, country)
    return clean_df


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-R",
        "--regions",
        help="Choose the region(s) you want to filter. Example: cleaning.py -R PT,US,FR",
        default="PT",
    )
    parser.add_argument(
        "-fn",
        "--file_name",
        help="Specify the file name to load. Example: cleaning.py -fn data/eu_life_expectancy_raw.tsv",
        default="eu_life_expectancy_raw.tsv",
    )
    args = parser.parse_args()
    main(**vars(args))
