"""Context module - Strategy Pattern"""

from life_expectancy.strategy import Strategy, FileTSV, FileJSON, Default
from typing import List, Dict
import pandas as pd


class Pipeline:
    def __init__(
        self,
        source_file: str = "data/eu_life_expectancy_raw.tsv",
        countries: List[str] = ["PT"],
    ):
        self.source_file: str = source_file
        self.countries: List[str] = countries
        self.strategies: Dict[str:Strategy] = {"TSV": FileTSV(), "JSON": FileJSON()}

    def setStrategy(self, strategy: Strategy = Default()) -> None:
        print("SetStrategy to ...")
        self.strategy = strategy
        print(self.strategy)

    def executeStrategy(self) -> pd.DataFrame:
        print(f"FILE TYPE: {self.source_file.split('.')[-1].upper()}")
        self.setStrategy(self.strategies[self.source_file.split(".")[-1].upper()])
        for country in self.countries:
            self.strategy.execute(self.source_file, country)
