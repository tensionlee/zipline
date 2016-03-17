"""
Dataset representing dates of upcoming dividends.
"""
from zipline.utils.numpy_utils import datetime64ns_dtype, float64_dtype

from .dataset import Column, DataSet


class _13DFilings(DataSet):
    previous_number_shares = Column(float64_dtype)
    previous_percentage = Column(float64_dtype)
    previous_disclosure_date = Column(datetime64ns_dtype)
