from zipline.pipeline.common import (
    EX_DATE_FIELD_NAME,
    PAY_DATE_FIELD_NAME,
    CASH_AMOUNT_FIELD_NAME,
    ANNOUNCEMENT_FIELD_NAME,
    DISCLOSURE_DATE, PERCENTAGE, NUM_SHARES)
from zipline.pipeline.loaders.events import EventsLoader
from zipline.pipeline.data.dividends import (
    DividendsByExDate,
    DividendsByAnnouncementDate,
    DividendsByPayDate
)
from zipline.utils.memoize import lazyval


class _13DFilingsLoader(EventsLoader):
    expected_cols = frozenset([DISCLOSURE_DATE,
                               PERCENTAGE,
                               NUM_SHARES])

    def __init__(self, all_dates, events_by_sid,
                 infer_timestamps=False,
                 dataset=DividendsByAnnouncementDate):
        super(_13DFilingsLoader, self).__init__(
            all_dates, events_by_sid, infer_timestamps, dataset=dataset,
        )

    @lazyval
    def previous_disclosure_date_loader(self):
        return self._previous_event_date_loader(
            self.dataset.previous_disclosure_date,
            DISCLOSURE_DATE
        )

    @lazyval
    def previous_percentage_loader(self):
        return self._previous_event_value_loader(
            self.dataset.previous_percentage,
            DISCLOSURE_DATE,
            PERCENTAGE
        )

    @lazyval
    def previous_number_shares_loader(self):
        return self._previous_event_value_loader(
            self.dataset.previous_number_shares,
            DISCLOSURE_DATE,
            NUM_SHARES
        )
