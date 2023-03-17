"""Python 3.11.2"""

from pathlib import Path
from typing import List, Dict
import pandas as pd

DIR_PATH = Path(__file__).parent


def _apply_unpivot(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Return Dataframe with the unpivots dates and desired columns"""
    id_vars = data_frame.columns[0]
    col_names = ["unit", "sex", "age", "region", "year", "value"]
    unpivot_df = pd.melt(frame=data_frame, id_vars=id_vars)
    unpivot_df[id_vars.split(",")] = unpivot_df[id_vars].str.split(",", expand=True)
    unpivot_df[col_names] = pd.concat(
        [unpivot_df[id_vars.split(",")], unpivot_df[["variable", "value"]]], axis=1
    )
    return unpivot_df[col_names]


def _apply_data_types(data_frame: pd.DataFrame) -> pd.DataFrame:
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


def clean_data(data_frame: pd.DataFrame, country: str = "PT") -> pd.DataFrame:
    """Main function to Clean Data and Filter Region"""
    clean_df = data_frame.pipe(_apply_unpivot).pipe(_apply_data_types)
    return clean_df[clean_df.region.str.upper() == country.upper()]
