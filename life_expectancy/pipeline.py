"""Pipeline module"""

import argparse
from life_expectancy.data import DataIOLoadJSON, DataIOLoadTSV


def main(*args, **kwargs) -> None:
    """Main Function which call functions of the data pipeline"""
    if args:
        data_pipe_tsv = DataIOLoadTSV()
        for country in args:
            clean_df = data_pipe_tsv.clean_data(data_pipe_tsv.load_data(), country)
            data_pipe_tsv.save_data(clean_df, country)
    if kwargs:
        if kwargs["file_name"].split(".")[-1].lower() == "tsv":
            data_pipe_tsv = DataIOLoadTSV()
            for country in kwargs["countries"].split(","):
                clean_df = data_pipe_tsv.clean_data(
                    data_pipe_tsv.load_data(kwargs["file_name"]), country
                )
                data_pipe_tsv.save_data(clean_df, country)

        if kwargs["file_name"].split(".")[-1].lower() == "json":
            data_pipe_json = DataIOLoadJSON()
            for country in kwargs["countries"].split(","):
                clean_df = data_pipe_json.clean_data(
                    data_pipe_json.load_data(kwargs["file_name"]), country
                )
                data_pipe_json.save_data(clean_df, country)
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
