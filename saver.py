import numpy as np
import pandas as pd
from typing import Dict
import logging

from paths import DATA_DIR


class Saver:
    def __init__(self, solutions: Dict[str, pd.DataFrame]) -> None:
        """ Initialize Saver instance

        :param solutions: dict with task_name and task solving result DataFrame
        :type solutions: Dict[str, pd.DataFrame]
        """
        self.solutions = solutions

    def save(self) -> None:
        """ Save all tasks solutions to .csv files
        :return: No return
        :rtype: None
        """
        for task, result in self.solutions.items():
            result.to_csv(f"{DATA_DIR}/results/{task}_solution.csv", index=False)
        logging.debug('All results dataframes are saved')
