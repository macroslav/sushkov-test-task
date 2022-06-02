from paths import DATA_DIR, PRODUCTS_PATH, SALES_PATH
from task_solvers import solve_first_task, solve_second_task, solve_third_task
from saver import Saver
from data_loader import DataLoader

import logging
from pathlib import Path

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

results = {}

products_path = Path(f'{DATA_DIR}/{PRODUCTS_PATH}')
sales_path = Path(f'{DATA_DIR}/{SALES_PATH}')

loader = DataLoader(products_path=products_path,
                    sales_path=sales_path)
sales_data, products_data, common_data = loader()

results['first_task'] = solve_first_task(common_data, products_data)
results['second_task'] = solve_second_task(common_data)

additional_orders_df = common_data[common_data.additional_order == 1].copy()

number_of_orders_df = common_data.groupby('customer_id', as_index=False) \
    .agg(number_of_customer_orders=('order_id', 'nunique')) \
    .sort_values(by='number_of_customer_orders', ascending=False)

additional_orders_df = additional_orders_df.merge(number_of_orders_df, how='left')

more_than_2_orders_df = additional_orders_df[additional_orders_df.number_of_customer_orders > 2]

results['third_task'] = solve_third_task(more_than_2_orders_df)

saver = Saver(results)
saver.save()
