"""Data module"""

from pathlib import Path
from typing import List, Dict
import pandas as pd

DIR_PATH = Path(__file__).parent


class DataIO:
    """Data Class context to load and save data"""

    # pylint: disable=too-few-public-methods

    def __init__(self) -> None:
        pass

    def save_data(self, data_frame: pd.DataFrame, country: str = "pt") -> None:
        """Function that saves the data into a local CSV file"""
        data_frame.to_csv(
            DIR_PATH.joinpath(f"data/{country.lower()}_life_expectancy.csv"),
            index=False,
        )


class DataIOLoadTSV(DataIO):
    """Sub Class to Load specific format of input files (here TSV)"""

    def __init__(self):
        super()

    def load_data(
        self,
        file_name: str = "data/eu_life_expectancy_raw.tsv",
    ) -> pd.DataFrame:
        """Load data from file and Return a Pandas DataFrame"""
        return pd.read_csv(DIR_PATH.joinpath(file_name), sep="\t")

    def _apply_unpivot(self, data_frame: pd.DataFrame) -> pd.DataFrame:
        """Return Dataframe with the unpivots dates and desired columns"""
        id_vars = data_frame.columns[0]
        col_names = ["unit", "sex", "age", "region", "year", "value"]
        unpivot_df = pd.melt(frame=data_frame, id_vars=id_vars)
        unpivot_df[id_vars.split(",")] = unpivot_df[id_vars].str.split(",", expand=True)
        unpivot_df[col_names] = pd.concat(
            [unpivot_df[id_vars.split(",")], unpivot_df[["variable", "value"]]], axis=1
        )
        return unpivot_df[col_names]

    def _apply_data_types(self, data_frame: pd.DataFrame) -> pd.DataFrame:
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

    def clean_data(self, data_frame: pd.DataFrame, country: str = "PT") -> pd.DataFrame:
        """Main function to Clean Data and Filter Countries"""
        clean_df = data_frame.pipe(self._apply_unpivot).pipe(self._apply_data_types)
        return clean_df[clean_df["region"].str.upper() == country.upper()]


class DataIOLoadJSON(DataIO):
    """Sub Class to Load specific format of input files (here TSV)"""

    def __init__(self):
        super()

    def load_data(
        self,
        file_name: str = "data/eurostat_life_expect.json",
    ) -> pd.DataFrame:
        """Load data from file and Return a Pandas DataFrame"""
        return pd.read_json(DIR_PATH.joinpath(file_name))

    def clean_data(self, data_frame: pd.DataFrame, country: str = "PT") -> pd.DataFrame:
        """Main function to Clean Data and Filter Countries"""
        return data_frame[data_frame["country"].str.upper() == country.upper()]
