"""Strategy module - Strategy Pattern"""

from abc import ABC, abstractmethod
from pathlib import Path
from enum import Enum
from dataclasses import dataclass
from typing import List, Dict
import pandas as pd


DIR_PATH = Path(__file__).parent


@dataclass
class Country(Enum):
    """Enum sub Class to define countries object"""

    # pylint: disable=invalid-name, no-method-argument
    PT: str = "PT"
    FR: str = "FR"
    NL: str = "NL"

    def possible_countries() -> List[str]:
        """Class method that return all the possible countries"""
        return [country.value for country in Country]


## Strategy interface
class Strategy(ABC):
    """Abstract Class to define Stategies classes"""

    # pylint: disable=dangerous-default-value
    @abstractmethod
    def load_data(self, source_file: str) -> pd.DataFrame:
        """Abstract `load_data` class method of Strategy class"""

    @abstractmethod
    def clean_data(self, data_frame: pd.DataFrame, countries: List[Country]) -> str:
        """Abstract `clean_data` class method of Strategy class"""

    @abstractmethod
    def execute(self, source_file: str, countries: List[Country]) -> pd.DataFrame:
        """Abstract `execute` class method of Strategy class"""

    def save_data(
        self, data_frame: pd.DataFrame, countries: List[Country] = [Country.PT]
    ) -> None:
        """Function that saves the data into a local CSV file"""
        data_frame.to_csv(
            DIR_PATH.joinpath(
                f"data/{('-').join([c.value for c in countries]).lower()}_life_expectancy.csv"
            ),
            index=False,
        )


## Concrete strategies
class FileTSV(Strategy):
    """Sub Class FileTSV of Abstract class Strategy to handle TSV files"""

    # pylint: disable=dangerous-default-value
    def load_data(
        self,
        source_file: str = "data/eu_life_expectancy_raw.tsv",
    ) -> pd.DataFrame:
        """Load data from file and Return a Pandas DataFrame"""
        return pd.read_csv(DIR_PATH.joinpath(source_file), sep="\t")

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

    def clean_data(
        self, data_frame: pd.DataFrame, countries: List[Country] = [Country.PT]
    ) -> pd.DataFrame:
        """Function to Clean Data and Filter Countries"""
        clean_df = data_frame.pipe(self._apply_unpivot).pipe(self._apply_data_types)
        return clean_df[
            clean_df["region"]
            .str.upper()
            .isin([country.value for country in countries])
        ]

    def execute(
        self,
        source_file: str = "data/eu_life_expectancy_raw.tsv",
        countries: List[Country] = [Country.PT],
    ) -> pd.DataFrame:
        """Function to execute FileTSV pipe"""
        raw_df = self.load_data(source_file)
        clean_df = self.clean_data(raw_df, countries)
        self.save_data(clean_df, countries)
        return clean_df


class FileJSON(Strategy):
    """Sub Class FileJSON of Abstract class Strategy to handle JSON files"""

    # pylint: disable=dangerous-default-value
    def load_data(
        self,
        source_file: str = "data/eurostat_life_expect.json",
    ) -> pd.DataFrame:
        """Load data from file and Return a Pandas DataFrame"""
        return pd.read_json(DIR_PATH.joinpath(source_file))

    def clean_data(
        self, data_frame: pd.DataFrame, countries: List[Country] = [Country.PT]
    ) -> pd.DataFrame:
        """Function to Clean Data and Filter Countries"""
        clean_df = data_frame[
            data_frame["country"]
            .str.upper()
            .isin([country.value for country in countries])
        ]
        clean_df = clean_df.mask(clean_df == "")
        return clean_df

    def execute(
        self,
        source_file: str = "data/eurostat_life_expect.json",
        countries: List[Country] = [Country.PT],
    ) -> pd.DataFrame:
        """Function to execute FileJSON pipe"""
        raw_df = self.load_data(source_file)
        clean_df = self.clean_data(raw_df, countries)
        self.save_data(clean_df, countries)
        return clean_df


class Default(Strategy):
    """Sub Class Default of Abstract class Strategy for default Strategy"""

    # pylint: disable=dangerous-default-value
    def load_data(
        self,
        source_file: str = "data/eu_life_expectancy_raw.tsv",
    ) -> pd.DataFrame:
        """Load data from file and Return a Pandas DataFrame for default Strategy"""
        return FileTSV().load_data(source_file)

    def clean_data(
        self, data_frame: pd.DataFrame, countries: List[Country] = [Country.PT]
    ) -> pd.DataFrame:
        """Function to Clean Data and Filter Countries for default Strategy"""
        return FileTSV().clean_data(data_frame, countries)

    def execute(
        self,
        source_file: str = "data/eu_life_expectancy_raw.tsv",
        countries: List[Country] = [Country.PT],
    ) -> pd.DataFrame:
        """Function to execute Default pipe"""
        return FileTSV().execute(source_file, countries)
