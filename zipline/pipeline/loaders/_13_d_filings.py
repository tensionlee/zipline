from zipline.pipeline.common import (
    DISCLOSURE_DATE, PERCENT_SHARES, NUM_SHARES)
from zipline.pipeline.loaders.events import EventsLoader
from zipline.pipeline.data._13_d_filings import _13DFilings
from zipline.utils.memoize import lazyval


class _13DFilingsLoader(EventsLoader):
    expected_cols = frozenset([DISCLOSURE_DATE,
                               PERCENT_SHARES,
                               NUM_SHARES])

    def __init__(self, all_dates, events_by_sid,
                 infer_timestamps=False,
                 dataset=_13DFilings):
        super(_13DFilingsLoader, self).__init__(
            all_dates, events_by_sid, infer_timestamps, dataset=dataset,
        )

    @lazyval
    def disclosure_date_loader(self):
        return self._previous_event_date_loader(
            self.dataset.disclosure_date,
            DISCLOSURE_DATE
        )

    @lazyval
    def percent_shares_loader(self):
        return self._previous_event_value_loader(
            self.dataset.percent_shares,
            DISCLOSURE_DATE,
            PERCENT_SHARES
        )

    @lazyval
    def number_shares_loader(self):
        return self._previous_event_value_loader(
            self.dataset.number_shares,
            DISCLOSURE_DATE,
            NUM_SHARES
        )
