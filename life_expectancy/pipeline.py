"""Pipeline module"""

import argparse
from life_expectancy.context import Pipeline


def main(*args, **kwargs) -> None:
    """Main Function which call functions of the context pipeline"""
    print(f"args: {args}")
    print(f"kwargs: {kwargs}")
    print(kwargs["countries"].split(","))
    if args:
        print("GO Args")
        clean_df = Pipeline().executeStrategy()

    if kwargs:
        print("GO Kwargs")
        clean_df = Pipeline(
            kwargs["file_name"], kwargs["countries"].split(",")
        ).executeStrategy()

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
