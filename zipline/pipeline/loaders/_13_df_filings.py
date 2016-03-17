from zipline.pipeline.common import (
    EX_DATE_FIELD_NAME,
    PAY_DATE_FIELD_NAME,
    CASH_AMOUNT_FIELD_NAME,
    ANNOUNCEMENT_FIELD_NAME,
    DISCLOSURE_DATE, PERCENT_SHARES, NUM_SHARES)
from zipline.pipeline.loaders.events import EventsLoader
from zipline.pipeline.data.dividends import (
    DividendsByExDate,
    DividendsByAnnouncementDate,
    DividendsByPayDate
)
from zipline.utils.memoize import lazyval


class _13DFilingsLoader(EventsLoader):
    expected_cols = frozenset([DISCLOSURE_DATE,
                               PERCENT_SHARES,
                               NUM_SHARES])

    def __init__(self, all_dates, events_by_sid,
                 infer_timestamps=False,
                 dataset=DividendsByAnnouncementDate):
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
