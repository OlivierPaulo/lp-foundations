"""Python 3.11.2"""

from life_expectancy.data import load_data, save_data
from life_expectancy.cleaning import clean_data
import argparse


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
        help="Specify data file name. Example: cleaning.py -fn data/eu_life_expectancy_raw.tsv",
        default="data/eu_life_expectancy_raw.tsv",
    )
    arguments = parser.parse_args()
    main(**vars(arguments))
