from zipline.pipeline.common import (
    ANNOUNCEMENT_FIELD_NAME,
    CASH_AMOUNT_FIELD_NAME,
    EX_DATE_FIELD_NAME,
    PAY_DATE_FIELD_NAME,
    SID_FIELD_NAME,
    TS_FIELD_NAME,
    PERCENTAGE, NUM_SHARES, DISCLOSURE_DATE)
from zipline.pipeline.data._13_d_filings import _13DFilings
from zipline.pipeline.data.dividends import DividendsByExDate, \
    DividendsByAnnouncementDate, DividendsByPayDate
from zipline.pipeline.loaders._13_df_filings import _13DFilingsLoader
from zipline.pipeline.loaders.dividends import DividendsByAnnouncementDateLoader, \
    DividendsByPayDateLoader, DividendsByExDateLoader
from .events import BlazeEventsLoader


class Blaze_13DFilingsLoader(BlazeEventsLoader):
    """A pipeline loader for the ``_13DFilings`` dataset that
    loads data from a blaze expression.

    Parameters
    ----------
    expr : Expr
        The expression representing the data to load.
    resources : dict, optional
        Mapping from the atomic terms of ``expr`` to actual data resources.
    odo_kwargs : dict, optional
        Extra keyword arguments to pass to odo when executing the expression.
    data_query_time : time, optional
        The time to use for the data query cutoff.
    data_query_tz : tzinfo or str
        The timezeone to use for the data query cutoff.
    dataset: DataSet
        The DataSet object for which this loader loads data.

    Notes
    -----
    The expression should have a tabular dshape of::

       Dim * {{
           {SID_FIELD_NAME}: int64,
           {TS_FIELD_NAME}: datetime,
           {PERCENTAGE}: float64,
           {NUM_SHARES}: float64,
           {DISCLOSURE_DATE}: ?datetime,
       }}

    Where each row of the table is a record including the sid to identify the
    company, the timestamp where we learned about the disclosure, the
    date of the disclosure, the percentage, and the number of shares.

    If the '{TS_FIELD_NAME}' field is not included it is assumed that we
    start the backtest with knowledge of all announcements.
    """

    __doc__ = __doc__.format(
        TS_FIELD_NAME=TS_FIELD_NAME,
        SID_FIELD_NAME=SID_FIELD_NAME,
        PERCENTAGE=PERCENTAGE,
        NUM_SHARES=NUM_SHARES,
        DISCLOSURE_DATE=DISCLOSURE_DATE
    )

    _expected_fields = frozenset({
        TS_FIELD_NAME,
        SID_FIELD_NAME,
        PERCENTAGE,
        NUM_SHARES,
        DISCLOSURE_DATE
    })

    concrete_loader = _13DFilingsLoader

    def __init__(self,
                 expr,
                 resources=None,
                 odo_kwargs=None,
                 data_query_time=None,
                 data_query_tz=None,
                 dataset=_13DFilings,
                 **kwargs):
        super(
            Blaze_13DFilingsLoader, self
        ).__init__(expr, dataset=dataset,
                   resources=resources, odo_kwargs=odo_kwargs,
                   data_query_time=data_query_time,
                   data_query_tz=data_query_tz, **kwargs)
