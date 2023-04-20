"""Context module pipeline - Strategy Pattern"""

from typing import List, Dict
import pandas as pd
from life_expectancy.strategy import Strategy, FileTSV, FileJSON, Default, Country


class Pipeline:
    """Context pipeline Class to apply then strategies"""

    # pylint: disable=too-few-public-methods, dangerous-default-value

    def __init__(
        self,
        source_file: str = "data/eu_life_expectancy_raw.tsv",
        countries: List[Country] = [Country.PT],
    ):
        self.source_file = source_file
        self.countries = countries
        self.strategies: Dict[str:Strategy] = {
            "TSV": FileTSV(),
            "JSON": FileJSON(),
            None: Default(),
        }
        self.strategy: Strategy = Default()

    def execute_strategy(self) -> pd.DataFrame:
        """Class method to execute the pipeline strategy"""
        self.strategy = self.strategies[self.source_file.split(".")[-1].upper()]
        return self.strategy.execute(self.source_file, self.countries)
