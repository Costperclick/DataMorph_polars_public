"""
Transformation pipeline : orchestrate transformations
"""

import polars as pl
from typing import Callable


class Transformer:
    def __init__(self, extract_data: pl.DataFrame, timestamp_date_str: str, specific_function:Callable=None):
        self.extract_data = extract_data
        self.timestamp_date = timestamp_date_str
        self.function = specific_function

    def run(self):
        if self.function:
            output = self.function(self.extract_data, self.timestamp_date)
            return output
        else:
            return f"No specific functions passed"
