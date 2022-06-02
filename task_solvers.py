import numpy as np
import pandas as pd

from typing import List, Tuple
from collections import defaultdict, Counter
import logging


def solve_first_task(data: pd.DataFrame, products: pd.DataFrame, task_type: str = 'amount') -> pd.DataFrame:
    """Return DataFrame with additional product and mean_ratio for each additional

    :param data: DataFrame with all data
    :type data: pd.DataFrame
    :param products: DataFrame with products ID and names
    :type products: pd.DataFrame
    :param task_type: the way how to count main products amount, allowed values 'amount' or 'unique', defaults to 'amount'
    :type task_type: str

    :rtype: pd.DataFrame
    :return: dataframe
    """

    data = data[data.additional_order == 1]

    if task_type == 'amount':

        main_quantity_df = data[data.additional_product == 0].groupby('order_id', as_index=False) \
            .agg(main_quantity=('amount', 'sum')) \
            .astype(int)
    else:

        main_quantity_df = data[data.additional_product == 0].groupby('order_id', as_index=False) \
            .agg(main_quantity=('product_id', 'count')) \
            .astype(int)

    additional_quantity_df = data[data.additional_product == 1].groupby(['order_id', 'product_id'], as_index=False) \
        .agg(additional_quantity=('amount', 'sum')) \
        .astype(int)

    main_with_additional = additional_quantity_df.merge(main_quantity_df, how='left')
    main_with_additional['additional_per_main'] = np.round(
        main_with_additional.additional_quantity / main_with_additional.main_quantity, 2)

    mean_by_additional = main_with_additional.groupby('product_id') \
        .agg(mean_additional_amount=('additional_per_main', 'mean')) \
        .reset_index() \
        .merge(products, how='left')
    logging.debug('First task is solved')
    return mean_by_additional


def solve_second_task(data: pd.DataFrame) -> pd.DataFrame:
    """ Return DataFrame with top-10 main products for each additional product

        :param data: DataFrame with all data
        :type data: pd.DataFrame

        :return: DataFrame with name of additional product and top-10 main products IDs
        :rtype: pd.DataFrame

    """

    def ranking_main(group: pd.DataFrame) -> None:
        """ Adds all occurring main products to dict

        :param group: pd.DataFrame grouped by order_id
        :type group: pd.DataFrame

        :return: None, add values to top_main dict
        :rtype: None

        """

        main_products = list(group[group.additional_product == 0].product_name.values)
        additional_products = list(group[group.additional_product == 1].product_name.values)
        for additional in additional_products:
            top_main[additional] += main_products

    top_main = defaultdict(list)

    data[data.additional_order == 1].groupby('order_id', as_index=False) \
        .apply(ranking_main)

    top_cols = [f'top_{i}' for i in range(1, 11)]
    top_10_df = pd.DataFrame(columns=[['additional_product'] + top_cols])

    for index, (key, value) in enumerate(top_main.items()):

        top_10_df.loc[index, 'additional_product'] = key
        top_10_main_counter = Counter(value).most_common(10)

        for main_product_tuple, top_col in zip(top_10_main_counter, top_cols):
            top_10_df.loc[index, top_col] = main_product_tuple[0]

    logging.debug('Second task is solved')

    return top_10_df


def solve_third_task(data: pd.DataFrame) -> pd.DataFrame:
    """ Return DataFrame with clients who order additional 3 times more often than mean client
    :param data: DataFrame with all data
    :type data: pd.DataFrame

    :return: Return DataFrame with clients who order additional 3 times more often than mean client
    :rtype: pd.DataFrame

    """

    customer_total_additional = data[data.additional_product == 1].groupby('customer_id', as_index=False) \
        .agg(total_additional_amount=('amount', 'sum')) \
        .astype(int)

    customer_total_main = data[data.additional_product == 0].groupby('customer_id', as_index=False) \
        .agg(total_main_amount=('amount', 'sum')) \
        .astype(int)

    customer_total_products = customer_total_additional.merge(customer_total_main, how='left')
    customer_total_products['total_additional_per_main'] = np.round(
        customer_total_products.total_additional_amount / customer_total_products.total_main_amount, 2)

    mean_ratio = customer_total_products.total_additional_per_main.mean()
    logging.debug('Third task is solved')

    return customer_total_products[
        customer_total_products.total_additional_per_main > 3 * mean_ratio]
