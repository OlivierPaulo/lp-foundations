"""Python 3.10.6"""

from pathlib import Path
from typing import List, Dict
import argparse
import pandas as pd

DIR_PATH = Path(__file__).parent


def load_data(
    file_path: str = "/data/eu_life_expectancy_raw.tsv",
) -> pd.DataFrame:
    """Load data from file and Return a Pandas DataFrame"""
    return pd.read_csv(f"{DIR_PATH}/{file_path}", sep="\t")


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


def clean_data(data_frame: pd.DataFrame, country: str) -> pd.DataFrame:
    """Main function to Clean Data and Filter Region"""
    clean_df = _apply_data_types(_apply_unpivot(data_frame))
    return clean_df[clean_df.region.str.upper() == country.upper()]


def _save_data(data_frame: pd.DataFrame, country: str = "pt") -> None:
    """Function that saves the data into a local CSV file"""
    data_frame.to_csv(f"{DIR_PATH}/data/{country}_life_expectancy.csv", index=False)


def main(country: str = "PT") -> None:
    """Main Function which call functions of the data pipeline"""
    raw_df = load_data()
    clean_df = clean_data(raw_df, country)
    _save_data(clean_df)


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-R",
        "--regions",
        help="Choose the region(s) you want to filter. Example : cleaning.py -R PT,US,FR",
    )
    args = parser.parse_args()
    if args.regions:
        for region in str(args.regions).split(","):
            main(region)
    else:
        main("PT")
