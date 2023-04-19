"""Context module pipeline - Strategy Pattern"""

from life_expectancy.strategy import Strategy, FileTSV, FileJSON, Default
from typing import Dict
import pandas as pd


class Pipeline:
    def __init__(
        self, source_file: str = "data/eu_life_expectancy_raw.tsv", country: str = "PT"
    ):
        self.source_file: str = source_file
        self.country: str = country
        self.strategies: Dict[str:Strategy] = {
            "TSV": FileTSV(),
            "JSON": FileJSON(),
            None: Default(),
        }

    def setStrategy(self, strategy: Strategy = Default()) -> None:
        self.strategy = strategy

    def executeStrategy(self) -> pd.DataFrame:
        self.setStrategy(self.strategies[self.source_file.split(".")[-1].upper()])
        return self.strategy.execute(self.source_file, self.country)
