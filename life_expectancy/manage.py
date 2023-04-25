"""Pipeline module"""

import argparse
from life_expectancy.pipelines import Pipeline, Country


def main(**kwargs) -> None:
    """Main Function which call functions of the context pipeline"""

    if not kwargs["countries"]:
        clean_df = Pipeline(source_file=kwargs["source_file"]).execute_strategy()
    else:
        clean_df = Pipeline(
            source_file=kwargs["source_file"],
            countries=[Country(country) for country in kwargs["countries"].split(",")],
        ).execute_strategy()

    return clean_df


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c",
        "--countries",
        help="Choose the countries(s) you want to filter. Example: manage.py -c PT,US,FR",
        # default="PT",
    )
    parser.add_argument(
        "-sf",
        "--source_file",
        help="Specify data file path. Example: manage.py -sf data/eu_life_expectancy_raw.tsv",
        default="data/eu_life_expectancy_raw.tsv",
    )
    arguments = parser.parse_args()
    main(**vars(arguments))
