"""
Tests for the reference loader for EarningsCalendar.
"""
from functools import partial
from unittest import TestCase

import blaze as bz
from blaze.compute.core import swap_resources_into_scope
from contextlib2 import ExitStack
import itertools
import pandas as pd
from six import iteritems
from tests.pipeline.base import EventLoaderCommonMixin

from zipline.pipeline.common import (
    ANNOUNCEMENT_FIELD_NAME,
    DAYS_SINCE_PREV_DIVIDEND_ANNOUNCEMENT,
    DAYS_SINCE_PREV_EX_DATE,
    DAYS_TO_NEXT_EX_DATE,
    NEXT_AMOUNT,
    NEXT_EX_DATE,
    NEXT_PAY_DATE,
    PREVIOUS_ANNOUNCEMENT,
    PREVIOUS_EX_DATE,
    PREVIOUS_PAY_DATE,
    PREVIOUS_AMOUNT,
    SID_FIELD_NAME,
    TS_FIELD_NAME,
    AD_FIELD_NAME,
    CASH_AMOUNT_FIELD_NAME,
    EX_DATE_FIELD_NAME,
    PAY_DATE_FIELD_NAME
)
from zipline.pipeline.data.dividends import DividendsByAnnouncementDate, \
    DividendsByExDate, DividendsByPayDate
from zipline.pipeline.factors.events import (
    BusinessDaysSinceDividendAnnouncement,
    BusinessDaysSincePreviousExDate,
    BusinessDaysUntilNextExDate
)
from zipline.pipeline.loaders.earnings import EarningsCalendarLoader
from zipline.pipeline.loaders.blaze import (
    BlazeEarningsCalendarLoader,
)
from zipline.utils.test_utils import (
    make_simple_equity_info,
    tmp_asset_finder,
)


dividends_cases = [
    # K1--K2--A1--A2.
    pd.DataFrame({
        CASH_AMOUNT_FIELD_NAME: [1, 15],
        EX_DATE_FIELD_NAME: pd.to_datetime(['2014-01-15', '2014-01-20']),
        PAY_DATE_FIELD_NAME: pd.to_datetime(['2014-01-15', '2014-01-20']),
        TS_FIELD_NAME: pd.to_datetime(['2014-01-05', '2014-01-10']),
        ANNOUNCEMENT_FIELD_NAME: pd.to_datetime(['2014-01-04', '2014-01-09'])
    }),
    # K1--K2--A2--A1.
    pd.DataFrame({
        CASH_AMOUNT_FIELD_NAME: [7, 13],
        EX_DATE_FIELD_NAME: pd.to_datetime(['2014-01-20', '2014-01-15']),
        PAY_DATE_FIELD_NAME: pd.to_datetime(['2014-01-20', '2014-01-15']),
        TS_FIELD_NAME: pd.to_datetime(['2014-01-05', '2014-01-10']),
        ANNOUNCEMENT_FIELD_NAME: pd.to_datetime(['2014-01-04', '2014-01-09'])
    }),
    # K1--A1--K2--A2.
    pd.DataFrame({
        CASH_AMOUNT_FIELD_NAME: [3, 1],
        EX_DATE_FIELD_NAME: pd.to_datetime(['2014-01-10', '2014-01-20']),
        PAY_DATE_FIELD_NAME: pd.to_datetime(['2014-01-10', '2014-01-20']),
        TS_FIELD_NAME: pd.to_datetime(['2014-01-05', '2014-01-15']),
        ANNOUNCEMENT_FIELD_NAME: pd.to_datetime(['2014-01-04', '2014-01-14'])
    }),
    # K1 == K2.
    pd.DataFrame({
        CASH_AMOUNT_FIELD_NAME: [6, 23],
        EX_DATE_FIELD_NAME: pd.to_datetime(['2014-01-10', '2014-01-15']),
        PAY_DATE_FIELD_NAME: pd.to_datetime(['2014-01-10', '2014-01-15']),
        TS_FIELD_NAME: pd.to_datetime(['2014-01-05'] * 2),
        ANNOUNCEMENT_FIELD_NAME: pd.to_datetime(['2014-01-04', '2014-01-04'])
    }),
    pd.DataFrame(
        columns=[CASH_AMOUNT_FIELD_NAME,
                 EX_DATE_FIELD_NAME,
                 PAY_DATE_FIELD_NAME,
                 TS_FIELD_NAME,
                 ANNOUNCEMENT_FIELD_NAME],
        dtype='datetime64[ns]'
    ),
]

date_intervals = [[None, '2014-01-14'], ['2014-01-15', '2014-01-19'],
                  ['2014-01-20', None]]


def get_values_for_date_ranges(zip_with_floats_dates,
                               num_days_between_dates,
                               vals_for_date_intervals):
    # Fill in given values for given date ranges.
    return zip_with_floats_dates(
        list(
            itertools.chain(*[
                [val] * num_days_between_dates(*date_intervals[i])
                for i, val in enumerate(vals_for_date_intervals)
            ])
        )
    )


def get_previous_amounts_for_dates(zip_with_floats_dates,
                                   num_days_between_dates,
                                   dates):
    return pd.DataFrame({
            0: get_values_for_date_ranges(zip_with_floats_dates,
                                          num_days_between_dates,
                                          ['NaN', 1, 15]),
            1: get_values_for_date_ranges(zip_with_floats_dates,
                                          num_days_between_dates,
                                          ['NaN', 13, 7]),
            2: get_values_for_date_ranges(zip_with_floats_dates,
                                          num_days_between_dates,
                                          ['NaN', 3, 1]),
            3: get_values_for_date_ranges(zip_with_floats_dates,
                                          num_days_between_dates,
                                          ['NaN', 6, 23]),
            4: zip_with_floats_dates(['NaN'] * len(dates)),
        }, index=dates)


class DividendsByAnnouncementDateTestCase(TestCase, EventLoaderCommonMixin):
    """
    Tests for loading the earnings announcement data.
    """
    pipeline_columns = {
        PREVIOUS_ANNOUNCEMENT:
            DividendsByAnnouncementDate.announcement_date.latest,
        PREVIOUS_AMOUNT: DividendsByAnnouncementDate.amount.latest,
        DAYS_SINCE_PREV_DIVIDEND_ANNOUNCEMENT:
            BusinessDaysSinceDividendAnnouncement(),
    }


    @classmethod
    def setUpClass(cls):
        cls._cleanup_stack = stack = ExitStack()
        equity_info = make_simple_equity_info(
            cls.sids,
            start_date=pd.Timestamp('2013-01-01', tz='UTC'),
            end_date=pd.Timestamp('2015-01-01', tz='UTC'),
        )
        cls.cols = {}
        cls.dataset = {sid:
                       frame.drop([EX_DATE_FIELD_NAME,
                                   PAY_DATE_FIELD_NAME], axis=1)
                       for sid, frame
                       in enumerate(dividends_cases)}
        cls.finder = stack.enter_context(
            tmp_asset_finder(equities=equity_info),
        )

        cls.loader_type = EarningsCalendarLoader

    @classmethod
    def tearDownClass(cls):
        cls._cleanup_stack.close()

    def setup(self, dates):
        zip_with_floats_dates = partial(self.zip_with_floats, dates)
        num_days_between_dates = partial(self.num_days_between, dates)
        num_days_between_for_dates = partial(self.num_days_between, dates)
        zip_with_dates_for_dates = partial(self.zip_with_dates, dates)
        self.cols[PREVIOUS_ANNOUNCEMENT] = pd.DataFrame({
            0: get_values_for_date_ranges(zip_with_floats_dates,
                                          num_days_between_for_dates,
                                          ['NaT', '2014-01-04', '2014-01-09']),
            1: get_values_for_date_ranges(zip_with_floats_dates,
                                          num_days_between_for_dates,
                                          ['NaT', '2014-01-04', '2014-01-09']),
            2: get_values_for_date_ranges(zip_with_floats_dates,
                                          num_days_between_for_dates,
                                          ['NaT', '2014-01-04', '2014-01-14']),
            3: get_values_for_date_ranges(zip_with_floats_dates,
                                          num_days_between_for_dates,
                                          ['NaT', '2014-01-04', '2014-01-04']),
            4: zip_with_dates_for_dates(['NaT'] * len(dates)),
        }, index=dates)

        self.cols[PREVIOUS_AMOUNT] = get_previous_amounts_for_dates(
            zip_with_floats_dates, num_days_between_dates, dates
        )

        self.cols[
            DAYS_SINCE_PREV_DIVIDEND_ANNOUNCEMENT
        ] = self._compute_busday_offsets(self.cols[PREVIOUS_ANNOUNCEMENT])


class DividendsByExDateTestCase(TestCase, EventLoaderCommonMixin):
    """
    Tests for loading the earnings announcement data.
    """
    pipeline_columns = {
        NEXT_EX_DATE: DividendsByExDate.previous_ex_date.latest,
        PREVIOUS_EX_DATE: DividendsByExDate.next_ex_date.latest,
        NEXT_AMOUNT: DividendsByExDate.next_amount.latest,
        PREVIOUS_AMOUNT: DividendsByExDate.previous_amount.latest,
        DAYS_TO_NEXT_EX_DATE: BusinessDaysUntilNextExDate(),
        DAYS_SINCE_PREV_EX_DATE: BusinessDaysSincePreviousExDate()
    }

    @classmethod
    def setUpClass(cls):
        cls._cleanup_stack = stack = ExitStack()
        equity_info = make_simple_equity_info(
            cls.sids,
            start_date=pd.Timestamp('2013-01-01', tz='UTC'),
            end_date=pd.Timestamp('2015-01-01', tz='UTC'),
        )
        cls.cols = {}
        cls.dataset = {sid:
                       frame.drop([ANNOUNCEMENT_FIELD_NAME,
                                   PAY_DATE_FIELD_NAME], axis=1)
                       for sid, frame
                       in enumerate(dividends_cases)}
        cls.finder = stack.enter_context(
            tmp_asset_finder(equities=equity_info),
        )

        cls.loader_type = EarningsCalendarLoader

    @classmethod
    def tearDownClass(cls):
        cls._cleanup_stack.close()

    def setup(self, dates):
        zip_with_floats_dates = partial(self.zip_with_floats, dates)
        num_days_between_dates = partial(self.num_days_between, dates)
        self.cols[NEXT_EX_DATE] = self.get_expected_previous_event_dates(dates)
        self.cols[PREVIOUS_EX_DATE] = self.get_expected_previous_event_dates(
            dates
        )

        # TODO: fix amounts for next/previous to correct ones
        _expected_next_amount = pd.DataFrame({
            0: zip_with_floats_dates(
                ['NaN'] * num_days_between_dates(None, '2014-01-14') +
                [1] * num_days_between_dates('2014-01-15', '2014-01-19') +
                [15] * num_days_between_dates('2014-01-20', None)
            ),
            1: zip_with_floats_dates(
                ['NaN'] * num_days_between_dates(None, '2014-01-14') +
                [13] * num_days_between_dates('2014-01-15', '2014-01-19') +
                [7] * num_days_between_dates('2014-01-20', None)
            ),
            2: zip_with_floats_dates(
                ['NaN'] * num_days_between_dates(None, '2014-01-09') +
                [3] * num_days_between_dates('2014-01-10', '2014-01-19') +
                [1] * num_days_between_dates('2014-01-20', None)
            ),
            3: zip_with_floats_dates(
                ['NaN'] * num_days_between_dates(None, '2014-01-09') +
                [6] * num_days_between_dates('2014-01-10', '2014-01-14') +
                [23] * num_days_between_dates('2014-01-15', None)
            ),
            4: zip_with_floats_dates(['NaN'] * len(dates)),
        }, index=dates)
        self.cols[NEXT_AMOUNT] = _expected_next_amount

        self.cols[PREVIOUS_AMOUNT] = get_previous_amounts_for_dates(
            zip_with_floats_dates, num_days_between_dates, dates
        )
        self.cols[DAYS_SINCE_PREV_DIVIDEND_ANNOUNCEMENT] = \
            self._compute_busday_offsets(
                self.cols[PREVIOUS_ANNOUNCEMENT]
            )

        self.cols[DAYS_TO_NEXT_EX_DATE] = self._compute_busday_offsets(
                self.cols[DAYS_TO_NEXT_EX_DATE]
        )

        self.cols[DAYS_SINCE_PREV_EX_DATE] = self._compute_busday_offsets(
                self.cols[DAYS_SINCE_PREV_EX_DATE]
        )


class DividendsByPayDateTestCase(TestCase, EventLoaderCommonMixin):
    """
    Tests for loading the earnings announcement data.
    """
    pipeline_columns = {
        NEXT_PAY_DATE: DividendsByPayDate.next_pay_date.latest,
        PREVIOUS_PAY_DATE: DividendsByPayDate.previous_pay_date.latest,
        NEXT_AMOUNT: DividendsByPayDate.next_amount.latest,
        PREVIOUS_AMOUNT: DividendsByPayDate.previous_amount.latest,
    }


    @classmethod
    def setUpClass(cls):
        cls._cleanup_stack = stack = ExitStack()
        equity_info = make_simple_equity_info(
            cls.sids,
            start_date=pd.Timestamp('2013-01-01', tz='UTC'),
            end_date=pd.Timestamp('2015-01-01', tz='UTC'),
        )
        cls.cols = {}
        cls.dataset = {sid:
                       frame.drop([ANNOUNCEMENT_FIELD_NAME,
                                   EX_DATE_FIELD_NAME], axis=1)
                       for sid, frame
                       in enumerate(dividends_cases)}
        cls.finder = stack.enter_context(
            tmp_asset_finder(equities=equity_info),
        )

        cls.loader_type = EarningsCalendarLoader

    @classmethod
    def tearDownClass(cls):
        cls._cleanup_stack.close()

    def setup(self, dates):
        zip_with_floats_dates = partial(self.zip_with_floats, dates)
        num_days_between_dates = partial(self.num_days_between, dates)
        self.cols[NEXT_PAY_DATE] = self.get_expected_next_event_dates(dates)
        self.cols[
            PREVIOUS_PAY_DATE
        ] = self.get_expected_previous_event_dates(dates)
        # TODO: fix amounts for next/previous to correct ones
        _expected_next_amount = pd.DataFrame({
            0: zip_with_floats_dates(
                ['NaN'] * num_days_between_dates(None, '2014-01-14') +
                [1] * num_days_between_dates('2014-01-15', '2014-01-19') +
                [15] * num_days_between_dates('2014-01-20', None)
            ),
            1: zip_with_floats_dates(
                ['NaN'] * num_days_between_dates(None, '2014-01-14') +
                [13] * num_days_between_dates('2014-01-15', '2014-01-19') +
                [7] * num_days_between_dates('2014-01-20', None)
            ),
            2: zip_with_floats_dates(
                ['NaN'] * num_days_between_dates(None, '2014-01-09') +
                [3] * num_days_between_dates('2014-01-10', '2014-01-19') +
                [1] * num_days_between_dates('2014-01-20', None)
            ),
            3: zip_with_floats_dates(
                ['NaN'] * num_days_between_dates(None, '2014-01-09') +
                [6] * num_days_between_dates('2014-01-10', '2014-01-14') +
                [23] * num_days_between_dates('2014-01-15', None)
            ),
            4: zip_with_floats_dates(['NaN'] * len(dates)),
        }, index=dates)
        self.cols[NEXT_AMOUNT] = _expected_next_amount
        _expected_previous_amount = pd.DataFrame({
            0: zip_with_floats_dates(
                ['NaN'] * num_days_between_dates(None, '2014-01-14') +
                [1] * num_days_between_dates('2014-01-15', '2014-01-19') +
                [15] * num_days_between_dates('2014-01-20', None)
            ),
            1: zip_with_floats_dates(
                ['NaN'] * num_days_between_dates(None, '2014-01-14') +
                [13] * num_days_between_dates('2014-01-15', '2014-01-19') +
                [7] * num_days_between_dates('2014-01-20', None)
            ),
            2: zip_with_floats_dates(
                ['NaN'] * num_days_between_dates(None, '2014-01-09') +
                [3] * num_days_between_dates('2014-01-10', '2014-01-19') +
                [1] * num_days_between_dates('2014-01-20', None)
            ),
            3: zip_with_floats_dates(
                ['NaN'] * num_days_between_dates(None, '2014-01-09') +
                [6] * num_days_between_dates('2014-01-10', '2014-01-14') +
                [23] * num_days_between_dates('2014-01-15', None)
            ),
            4: zip_with_floats_dates(['NaN'] * len(dates)),
        }, index=dates)
        self.cols[PREVIOUS_AMOUNT] = _expected_previous_amount


class BlazeEarningsCalendarLoaderTestCase(EarningsCalendarLoaderTestCase):
    @classmethod
    def setUpClass(cls):
        super(BlazeEarningsCalendarLoaderTestCase, cls).setUpClass()
        cls.loader_type = BlazeEarningsCalendarLoader

    def loader_args(self, dates):
        _, mapping = super(
            BlazeEarningsCalendarLoaderTestCase,
            self,
        ).loader_args(dates)
        return (bz.Data(pd.concat(
            pd.DataFrame({
                ANNOUNCEMENT_FIELD_NAME: df[ANNOUNCEMENT_FIELD_NAME],
                TS_FIELD_NAME: df[TS_FIELD_NAME],
                SID_FIELD_NAME: sid,
            })
            for sid, df in iteritems(mapping)
        ).reset_index(drop=True)),)


class BlazeEarningsCalendarLoaderNotInteractiveTestCase(
        BlazeEarningsCalendarLoaderTestCase):
    """Test case for passing a non-interactive symbol and a dict of resources.
    """
    @classmethod
    def setUpClass(cls):
        super(BlazeEarningsCalendarLoaderNotInteractiveTestCase,
              cls).setUpClass()
        cls.loader_type = BlazeEarningsCalendarLoader

    def loader_args(self, dates):
        (bound_expr,) = super(
            BlazeEarningsCalendarLoaderNotInteractiveTestCase,
            self,
        ).loader_args(dates)
        return swap_resources_into_scope(bound_expr, {})
