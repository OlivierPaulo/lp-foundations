"""Pipeline module"""

import argparse
from life_expectancy.context import Pipeline
from life_expectancy.strategy import Country


def main(*args, **kwargs) -> None:
    """Main Function which call functions of the context pipeline"""

    if args:
        clean_df = Pipeline().execute_strategy()

    if kwargs:
        clean_df = Pipeline(
            kwargs["file_name"],
            [Country[country] for country in kwargs["countries"].split(",")],
        ).execute_strategy()

    return clean_df


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--countries",
        help="Choose the countries(s) you want to filter. Example: pipeline.py -c PT,US,FR",
        default="PT",
    )
    parser.add_argument(
        "-fn",
        "--file_name",
        help="Specify data file path. Example: pipeline.py -fn data/eu_life_expectancy_raw.tsv",
        default="data/eu_life_expectancy_raw.tsv",
    )
    arguments = parser.parse_args()
    main(**vars(arguments))
