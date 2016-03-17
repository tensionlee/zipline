"""
Tests for the reference loader for Buyback Authorizations.
"""
from unittest import TestCase

import blaze as bz
from blaze.compute.core import swap_resources_into_scope
from contextlib2 import ExitStack
import pandas as pd
from six import iteritems
from .base import EventLoaderCommonMixin

from zipline.pipeline.common import(
    BUYBACK_ANNOUNCEMENT_FIELD_NAME,
    CASH_FIELD_NAME,
    DAYS_SINCE_PREV,
    PREVIOUS_BUYBACK_ANNOUNCEMENT,
    PREVIOUS_BUYBACK_CASH,
    PREVIOUS_BUYBACK_SHARE_COUNT,
    SHARE_COUNT_FIELD_NAME,
    SID_FIELD_NAME,
    TS_FIELD_NAME
)
from zipline.pipeline.data import (
    CashBuybackAuthorizations,
    ShareBuybackAuthorizations
)
from zipline.pipeline.factors.events import (
    BusinessDaysSinceCashBuybackAuth,
    BusinessDaysSinceShareBuybackAuth
)
from zipline.pipeline.loaders.buyback_auth import (
    CashBuybackAuthorizationsLoader,
    ShareBuybackAuthorizationsLoader
)
from zipline.pipeline.loaders.blaze import (
    BlazeCashBuybackAuthorizationsLoader,
    BlazeShareBuybackAuthorizationsLoader,
)
from zipline.pipeline.loaders.utils import get_values_for_date_ranges, \
    zip_with_floats, zip_with_dates
from zipline.testing import tmp_asset_finder

date_intervals = [[None, '2014-01-04'], ['2014-01-05', '2014-01-09'],
                  ['2014-01-10', None]]

buyback_authorizations_cases = [
    pd.DataFrame({
        SHARE_COUNT_FIELD_NAME: [1, 15],
        CASH_FIELD_NAME: [10, 20],
        TS_FIELD_NAME: pd.to_datetime(['2014-01-05', '2014-01-10']),
        BUYBACK_ANNOUNCEMENT_FIELD_NAME: pd.to_datetime(['2014-01-04',
                                                         '2014-01-09'])
    }),
    pd.DataFrame(
        columns=[SHARE_COUNT_FIELD_NAME,
                 CASH_FIELD_NAME,
                 BUYBACK_ANNOUNCEMENT_FIELD_NAME,
                 TS_FIELD_NAME],
        dtype='datetime64[ns]'
    ),
]


def get_expected_previous_values(zip_date_index_with_vals,
                                 dates,
                                 vals_for_date_intervals):
    return pd.DataFrame({
        0: get_values_for_date_ranges(zip_date_index_with_vals,
                                      vals_for_date_intervals,
                                      date_intervals,
                                      dates),
        1: zip_date_index_with_vals(dates, ['NaN'] * len(dates)),
    }, index=dates)


class CashBuybackAuthLoaderTestCase(TestCase, EventLoaderCommonMixin):
    """
    Test for cash buyback authorizations dataset.
    """
    pipeline_columns = {
        PREVIOUS_BUYBACK_CASH:
            CashBuybackAuthorizations.cash_amount.latest,
        PREVIOUS_BUYBACK_ANNOUNCEMENT:
            CashBuybackAuthorizations.announcement_date.latest,
        DAYS_SINCE_PREV:
            BusinessDaysSinceCashBuybackAuth(),
    }

    @classmethod
    def get_sids(cls):
        return range(2)

    @classmethod
    def setUpClass(cls):
        cls._cleanup_stack = stack = ExitStack()
        cls.finder = stack.enter_context(
            tmp_asset_finder(equities=cls.get_equity_info()),
        )
        cls.cols = {}
        cls.dataset = {sid:
                       frame.drop(SHARE_COUNT_FIELD_NAME, axis=1)
                       for sid, frame
                       in enumerate(buyback_authorizations_cases)}
        cls.loader_type = CashBuybackAuthorizationsLoader

    @classmethod
    def tearDownClass(cls):
        cls._cleanup_stack.close()

    def setup(self, dates):
        _expected_previous_cash = get_expected_previous_values(
            zip_with_floats, dates,
            ['NaN', 10, 20]
        )
        self.cols[
            PREVIOUS_BUYBACK_ANNOUNCEMENT
        ] = get_expected_previous_values(zip_with_dates, dates,
                                         ['NaT', '2014-01-04', '2014-01-09'])
        self.cols[PREVIOUS_BUYBACK_CASH] = _expected_previous_cash
        self.cols[DAYS_SINCE_PREV] = self._compute_busday_offsets(
            self.cols[PREVIOUS_BUYBACK_ANNOUNCEMENT]
        )


class ShareBuybackAuthLoaderTestCase(TestCase, EventLoaderCommonMixin):
    """
    Test for share buyback authorizations dataset.
    """
    pipeline_columns = {
        PREVIOUS_BUYBACK_SHARE_COUNT:
            ShareBuybackAuthorizations.share_count.latest,
        PREVIOUS_BUYBACK_ANNOUNCEMENT:
            ShareBuybackAuthorizations.announcement_date.latest,
        DAYS_SINCE_PREV:
            BusinessDaysSinceShareBuybackAuth(),
    }

    @classmethod
    def get_sids(cls):
        return range(2)

    @classmethod
    def setUpClass(cls):
        cls._cleanup_stack = stack = ExitStack()
        cls.finder = stack.enter_context(
            tmp_asset_finder(equities=cls.get_equity_info()),
        )
        cls.cols = {}
        cls.dataset = {sid:
                       frame.drop(CASH_FIELD_NAME, axis=1)
                       for sid, frame
                       in enumerate(buyback_authorizations_cases)}
        cls.loader_type = ShareBuybackAuthorizationsLoader

    @classmethod
    def tearDownClass(cls):
        cls._cleanup_stack.close()

    def setup(self, dates):
        self.cols[
            PREVIOUS_BUYBACK_SHARE_COUNT
        ] = get_expected_previous_values(zip_with_floats,
                                         dates,
                                         ['NaN', 1, 15])
        self.cols[
            PREVIOUS_BUYBACK_ANNOUNCEMENT
        ] = get_expected_previous_values(zip_with_dates, dates,
                                         ['NaT', '2014-01-04', '2014-01-09'])
        self.cols[DAYS_SINCE_PREV] = self._compute_busday_offsets(
            self.cols[PREVIOUS_BUYBACK_ANNOUNCEMENT]
        )


class BlazeCashBuybackAuthLoaderTestCase(CashBuybackAuthLoaderTestCase):
    """ Test case for loading via blaze.
    """
    @classmethod
    def setUpClass(cls):
        super(BlazeCashBuybackAuthLoaderTestCase, cls).setUpClass()
        cls.loader_type = BlazeCashBuybackAuthorizationsLoader

    def loader_args(self, dates):
        _, mapping = super(
            BlazeCashBuybackAuthLoaderTestCase,
            self,
        ).loader_args(dates)
        return (bz.data(pd.concat(
            pd.DataFrame({
                BUYBACK_ANNOUNCEMENT_FIELD_NAME:
                    frame[BUYBACK_ANNOUNCEMENT_FIELD_NAME],
                CASH_FIELD_NAME:
                    frame[CASH_FIELD_NAME],
                TS_FIELD_NAME:
                    frame[TS_FIELD_NAME],
                SID_FIELD_NAME: sid,
            })
            for sid, frame in iteritems(mapping)
        ).reset_index(drop=True)),)


class BlazeShareBuybackAuthLoaderTestCase(ShareBuybackAuthLoaderTestCase):
    """ Test case for loading via blaze.
    """
    @classmethod
    def setUpClass(cls):
        super(BlazeShareBuybackAuthLoaderTestCase, cls).setUpClass()
        cls.loader_type = BlazeShareBuybackAuthorizationsLoader

    def loader_args(self, dates):
        _, mapping = super(
            BlazeShareBuybackAuthLoaderTestCase,
            self,
        ).loader_args(dates)
        return (bz.data(pd.concat(
            pd.DataFrame({
                BUYBACK_ANNOUNCEMENT_FIELD_NAME:
                    frame[BUYBACK_ANNOUNCEMENT_FIELD_NAME],
                SHARE_COUNT_FIELD_NAME:
                    frame[SHARE_COUNT_FIELD_NAME],
                TS_FIELD_NAME:
                    frame[TS_FIELD_NAME],
                SID_FIELD_NAME: sid,
            })
            for sid, frame in iteritems(mapping)
        ).reset_index(drop=True)),)


class BlazeShareBuybackAuthLoaderNotInteractiveTestCase(
        BlazeShareBuybackAuthLoaderTestCase):
    """Test case for passing a non-interactive symbol and a dict of resources.
    """
    def loader_args(self, dates):
        (bound_expr,) = super(
            BlazeShareBuybackAuthLoaderNotInteractiveTestCase,
            self,
        ).loader_args(dates)
        return swap_resources_into_scope(bound_expr, {})


class BlazeCashBuybackAuthLoaderNotInteractiveTestCase(
        BlazeCashBuybackAuthLoaderTestCase):
    """Test case for passing a non-interactive symbol and a dict of resources.
    """
    def loader_args(self, dates):
        (bound_expr,) = super(
            BlazeCashBuybackAuthLoaderNotInteractiveTestCase,
            self,
        ).loader_args(dates)
        return swap_resources_into_scope(bound_expr, {})
