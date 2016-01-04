"""
Dataset representing dates of upcoming dividends.
"""
from zipline.utils.numpy_utils import datetime64ns_dtype, float64_dtype

from .dataset import Column, DataSet


class _Dividends(DataSet):
    """
    Dataset representing dates of upcoming or recently announced dividends.
    """
    next_ex_date = Column(datetime64ns_dtype)
    previous_ex_date = Column(datetime64ns_dtype)
    next_pay_date = Column(datetime64ns_dtype)
    previous_pay_date = Column(datetime64ns_dtype)
    next_record_date = Column(datetime64ns_dtype)
    previous_record_date = Column(datetime64ns_dtype)


class CashDividends(_Dividends):
    """
    Dataset representing dates and amounts for upcoming and recently
    announced dividends.
    """
    next_amount = Column(float64_dtype)
    previous_amount = Column(float64_dtype)
