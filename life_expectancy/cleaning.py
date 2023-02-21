"""Python 3.10.6"""

from pathlib import Path
from typing import List, Dict
import sys
import pandas as pd

DIR_PATH = f'{Path(__file__).parent}/data'


def load_data(file_path: str = f'{DIR_PATH}/eu_life_expectancy_raw.tsv') \
        -> pd.DataFrame:
    """Load data from file and Return a Pandas DataFrame"""
    return pd.read_csv(Path(file_path), sep='\t')


def apply_unpivot(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Return Dataframe with the unpivots dates and desired columns"""
    id_vars = data_frame.columns[0]
    col_names = ['unit', 'sex', 'age', 'region', 'year', 'value']
    unpivot_df = pd.melt(
                        frame=data_frame,
                        id_vars=id_vars
                    )
    unpivot_df[id_vars.split(',')] = unpivot_df[id_vars].str.split(',', expand=True)
    unpivot_df[col_names] = pd.concat([unpivot_df[id_vars.split(',')],
                                       unpivot_df[['variable', 'value']]],axis=1)
    return unpivot_df[col_names]


def apply_data_types(data_frame: pd.DataFrame) -> pd.DataFrame:
    """Ensure data types defines by type_rules and remove NaNs for requested cols"""
    types_rules: Dict[str, object] = {'year': int, 'value': float}
    cols_to_delete:  List[str] = ['value']
    for column, data_type in types_rules.items():
        data_frame[column] = pd.to_numeric(data_frame[column].str.extractall(r'(\d+.\d+)')
                                                .astype(data_type).unstack()
                                                .max(axis=1),
                                            errors='coerce')
    return data_frame.dropna(subset=cols_to_delete)


def clean_data(region: str = 'PT',
               data_frame: pd.DataFrame = load_data()) -> None:
    """Main function to Clean Data and Filter Region"""
    clean_df = apply_data_types(apply_unpivot(data_frame))
    if sys.argv and len(sys.argv) > 1:
        filter_df = clean_df[clean_df.region.str.upper() == sys.argv[1].upper()]
    filter_df = clean_df[clean_df.region.str.upper() == region.upper()]
    save_data(filter_df)
    return filter_df


def save_data(data_frame: pd.DataFrame) -> None:
    """Function that saves the data into a local CSV file"""
    data_frame.to_csv(Path(f'{DIR_PATH}/pt_life_expectancy.csv'), index=False)


def main() -> None:
    """Main Function which call functions of the data pipeline"""
    data_frame = load_data()
    data_frame = clean_data("PT", data_frame)
    save_data(data_frame)


if __name__ == "__main__":  # pragma: no cover
    main()
