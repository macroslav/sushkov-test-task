import pandas as pd

from typing import Tuple
import logging


def convert_to_float(excel_float: str):
    """
    Convert Excel comma separated float to python float

    :param excel_float: string with Excel float value
    :type excel_float: str

    :return: correct python float value
    :rtype: float

    """
    python_float = float(excel_float.replace(',', '.'))
    return python_float


class DataLoader:
    """ Class for loading and basic preprocessing data from .csv files"""

    def __init__(self, products_path: str, sales_path: str) -> None:
        """ Initialize DataLoader

        :param products_path: path to products.csv
        :type products_path: str
        :param sales_path: path to sales.csv
        :type sales_path: str
        """
        self.products_path = products_path
        self.sales_path = sales_path
        self.products_df = None
        self.sales_df = None
        self.common_df = None

    def load(self) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """ Load data and preprocess it

        :return: sales, products and common DataFrame
        :rtype: Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]

        """
        self.sales_df, self.products_df = self._load_from_csv()
        self._preprocess()
        return self.sales_df, self.products_df, self.common_df

    def _load_from_csv(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """ Load data from .csv and save to class attributes"""
        products_df = pd.read_csv(self.products_path)
        sales_df = pd.read_csv(self.sales_path)
        logging.debug('Successfully load from .csv')
        return sales_df, products_df

    def _preprocess(self) -> None:
        """ Basic preprocessing of products_df and sales_df and merging their to common_df"""
        self.products_df = self.products_df.rename(columns={'id': 'product_id',
                                                            'title': 'product_name'
                                                            })
        self.sales_df = self.sales_df.rename(columns={'count': 'amount',
                                                      'is_additional': 'additional_product'
                                                      })
        self.sales_df.loc[:, 'amount'] = self.sales_df.amount.apply(convert_to_float)
        self.sales_df['additional_order'] = self.sales_df.groupby('order_id', as_index=False)['additional_product'] \
            .transform('max')

        self.common_df = self.sales_df.merge(self.products_df, how='left')
        logging.debug('Successfully preprocessed data')
