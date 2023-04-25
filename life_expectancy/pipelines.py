"""Context module pipeline - Strategy Pattern"""

from typing import List, Dict
import pandas as pd
from abc import ABC, abstractmethod
from pathlib import Path
from enum import Enum, auto
from dataclasses import dataclass
import pandas as pd


DIR_PATH = Path(__file__).parent


@dataclass
class Country(Enum):
    """Enum sub Class to define countries object"""

    AL = auto()
    AM = auto()
    AT = auto()
    AZ = auto()
    BE = auto()
    BG = auto()
    BY = auto()
    CH = auto()
    CY = auto()
    CZ = auto()
    DE = auto()
    DK = auto()
    EE = auto()
    EL = auto()
    ES = auto()
    FI = auto()
    FR = auto()
    FX = auto()
    GE = auto()
    HR = auto()
    HU = auto()
    IE = auto()
    IS = auto()
    IT = auto()
    LI = auto()
    LT = auto()
    LU = auto()
    LV = auto()
    MD = auto()
    ME = auto()
    MK = auto()
    MT = auto()
    NL = auto()
    NO = auto()
    PL = auto()
    PT = auto()
    RO = auto()
    RS = auto()
    RU = auto()
    SE = auto()
    SI = auto()
    SK = auto()
    SM = auto()
    TR = auto()
    UA = auto()
    UK = auto()
    XK = auto()

    @classmethod
    def possible_countries(self) -> list[str]:
        """Class method that return all the possible countries"""
        return [country.value for country in Country]


class Strategy(ABC):
    """Abstract Class to define Stategies classes"""

    @abstractmethod
    def load_data(self, source_file: str) -> pd.DataFrame:
        """Abstract `load_data` class method of Strategy class"""

    @abstractmethod
    def clean_data(self, data_frame: pd.DataFrame, countries: list[Country]) -> str:
        """Abstract `clean_data` class method of Strategy class"""

    @abstractmethod
    def execute(self, source_file: str, countries: list[Country]) -> pd.DataFrame:
        """Abstract `execute` class method of Strategy class"""

    def save_data(self, data_frame: pd.DataFrame, countries: list[Country]) -> None:
        """Function that saves the data into a local CSV file"""
        if not countries:
            data_frame.to_csv(
                DIR_PATH.joinpath(f"data/eu_life_expectancy.csv"),
                index=False,
            )
        else:
            data_frame.to_csv(
                DIR_PATH.joinpath(
                    f"data/{('-').join([c.value for c in countries]).lower()}_life_expectancy.csv"
                ),
                index=False,
            )

    def execute(
        self,
        source_file: str = "data/eu_life_expectancy_raw.tsv",
        countries: list[Country] = None,
    ) -> pd.DataFrame:
        """Function to execute pipeline"""
        raw_df = self.load_data(source_file)
        clean_df = self.clean_data(raw_df, countries)
        self.save_data(clean_df, countries)
        return clean_df


class FileTSV(Strategy):
    """ETL for TSV files"""

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
        self, data_frame: pd.DataFrame, countries: list[Country]
    ) -> pd.DataFrame:
        """Function to Clean Data and Filter Countries"""
        clean_df = data_frame.pipe(self._apply_unpivot).pipe(self._apply_data_types)
        if not countries:
            return clean_df
        return clean_df[
            clean_df["region"]
            .str.upper()
            .isin([country.value for country in countries])
        ]


class FileJSON(Strategy):
    """ETL for JSON files"""

    def load_data(
        self,
        source_file: str = "data/eurostat_life_expect.json",
    ) -> pd.DataFrame:
        """Load data from file and Return a Pandas DataFrame"""
        return pd.read_json(DIR_PATH.joinpath(source_file))

    def clean_data(
        self, data_frame: pd.DataFrame, countries: list[Country]
    ) -> pd.DataFrame:
        """Function to Clean Data and Filter Countries"""
        clean_df = data_frame.mask(data_frame == "")
        if not countries:
            return clean_df
        clean_df = clean_df[
            clean_df["country"]
            .str.upper()
            .isin([country.value for country in countries])
        ]
        return clean_df


class Pipeline:
    """Context pipeline Class to apply then strategies"""

    def __init__(
        self,
        source_file: Path | str = "data/eu_life_expectancy_raw.tsv",
        countries: list[Country] = None,
    ):
        self.source_file = source_file
        self.countries = countries
        self.strategies: Dict[str:Strategy] = {
            "TSV": FileTSV(),
            "JSON": FileJSON(),
            None: FileTSV(),
        }
        self.strategy: Strategy = FileTSV()

    def execute_strategy(self) -> pd.DataFrame:
        """Class method to execute the pipeline strategy"""
        self.strategy = self.strategies[self.source_file.split(".")[-1].upper()]
        return self.strategy.execute(
            source_file=self.source_file, countries=self.countries
        )
