import pandas as pd
import sqlalchemy as sa
from toolz import valmap, merge

from .base import PipelineLoader
from .earnings import next_earnings_date_frame, previous_earnings_date_frame
from zipline.pipeline.loaders.synthetic import DataFrameLoader
from zipline.pipeline.data.dividends import CashDividends


class CashDividendsLoader(PipelineLoader):
    def __init__(self, table, dataset=CashDividends):
        self._table = table
        self.dataset = dataset
        self._column_map = {
            dataset.next_ex_date: table.c.ex_date,
            dataset.previous_ex_date: table.c.ex_date,
            dataset.next_pay_date: table.c.pay_date,
            dataset.previous_pay_date: table.c.pay_date,
            dataset.next_record_date: table.c.record_date,
            dataset.previous_record_date: table.c.record_date,
        }

    def _load_raw(self, columns):
        table = self._table
        to_query = list(
            {self._column_map[column] for column in columns} |
            {table.c.declared_date, table.c.sid}
        )
        return pd.DataFrame.from_records(
            list(sa.select(to_query).execute()),
            columns=[column.name for column in to_query],
            coerce_floats=True,
        )

    @staticmethod
    def _load_date(column, dates, assets, raw, gb, mask):
        name = column.name
        if name.startswith('next_'):
            prefix = 'next_'
            to_frame = next_earnings_date_frame
        elif name.startswith('previous_'):
            prefix = 'previous_'
            to_frame = previous_earnings_date_frame
        else:
            raise AssertionError(
                "column name should start with 'next_' or 'previous_',"
                ' got %r' % name,
            )

        def mkseries(idx, raw_loc=raw.loc):
            vs = raw_loc[
                idx, ['declared_date', name[len(prefix):]],
            ].values
            return pd.Series(
                index=pd.DatetimeIndex(vs[:, 0]),
                data=vs[:, 1],
            )

        return DataFrameLoader(
            column,
            to_frame(
                dates,
                valmap(mkseries, gb.groups),
            ),
            adjustments=None,
        ).load_adjustmented_array([column], dates, assets, mask)

    def load_adjustmented_array(self, columns, dates, assets, mask):
        if set(columns).isdisjoint(self.dataset.columns):
            raise ValueError(
                'columns could not be loaded: %r' %
                set(columns).symetric_difference(self.dataset.columns),
            )

        raw = self._load_raw(columns)
        gb = raw[raw['sid'].isin(assets)].groupby('sid')
        return merge(*(
            self._load_date(column, dates, assets, raw, gb, mask)
            for column in columns
        ))
