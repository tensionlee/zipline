from zipline.pipeline.common import (
    EX_DATE_FIELD_NAME,
    PAY_DATE_FIELD_NAME,
    CASH_AMOUNT_FIELD_NAME,
    ANNOUNCEMENT_FIELD_NAME
)
from zipline.pipeline.loaders.events import EventsLoader
from zipline.pipeline.data.dividends import (
    DividendsByExDate,
    DividendsByAnnouncementDate,
    DividendsByPayDate
)
from zipline.utils.memoize import lazyval


class DividendsByAnnouncementDateLoader(EventsLoader):
    expected_cols = frozenset([ANNOUNCEMENT_FIELD_NAME,
                               CASH_AMOUNT_FIELD_NAME])

    def __init__(self, all_dates, events_by_sid,
                 infer_timestamps=False,
                 dataset=DividendsByAnnouncementDate):
        super(DividendsByAnnouncementDateLoader, self).__init__(
            all_dates, events_by_sid, infer_timestamps, dataset=dataset,
        )

    @lazyval
    def previous_announcement_date_loader(self):
        return self._previous_event_date_loader(
            self.dataset.previous_announcement_date,
            ANNOUNCEMENT_FIELD_NAME
        )

    @lazyval
    def previous_amount_loader(self):
        return self._previous_event_value_loader(
            self.dataset.previous_amount,
            ANNOUNCEMENT_FIELD_NAME,
            CASH_AMOUNT_FIELD_NAME
        )


class DividendsByPayDateLoader(EventsLoader):
    expected_cols = frozenset([EX_DATE_FIELD_NAME,
                               CASH_AMOUNT_FIELD_NAME])

    def __init__(self, all_dates, events_by_sid,
                 infer_timestamps=False,
                 dataset=DividendsByPayDate):
        super(DividendsByPayDateLoader, self).__init__(
            all_dates, events_by_sid, infer_timestamps, dataset=dataset,
        )

    @lazyval
    def next_pay_date_loader(self):
        return self._next_event_date_loader(self.dataset.next_pay_date,
                                            PAY_DATE_FIELD_NAME)

    @lazyval
    def previous_pay_date_loader(self):
        return self._previous_event_date_loader(
            self.dataset.previous_pay_date,
            PAY_DATE_FIELD_NAME
        )

    @lazyval
    def next_amount_loader(self):
        return self._next_event_value_loader(self.dataset.next_amount_date,
                                             PAY_DATE_FIELD_NAME,
                                             CASH_AMOUNT_FIELD_NAME)

    @lazyval
    def previous_amount_loader(self):
        return self._previous_event_value_loader(
            self.dataset.previous_amount_date,
            PAY_DATE_FIELD_NAME,
            CASH_AMOUNT_FIELD_NAME
        )


class DividendsByExDateLoader(EventsLoader):
    expected_cols = frozenset([EX_DATE_FIELD_NAME,
                               CASH_AMOUNT_FIELD_NAME])

    def __init__(self, all_dates, events_by_sid,
                 infer_timestamps=False,
                 dataset=DividendsByExDate):
        super(DividendsByExDateLoader, self).__init__(
            all_dates, events_by_sid, infer_timestamps, dataset=dataset,
        )

    @lazyval
    def next_ex_date_loader(self):
        return self._next_event_date_loader(self.dataset.next_ex_date,
                                            EX_DATE_FIELD_NAME)

    @lazyval
    def previous_ex_date_loader(self):
        return self._previous_event_date_loader(
            self.dataset.previous_ex_date,
            EX_DATE_FIELD_NAME
        )

    @lazyval
    def next_amount_loader(self):
        return self._next_event_value_loader(self.dataset.next_amount,
                                             EX_DATE_FIELD_NAME,
                                             CASH_AMOUNT_FIELD_NAME)

    @lazyval
    def previous_amount_loader(self):
        return self._previous_event_value_loader(
            self.dataset.previous_amount,
            EX_DATE_FIELD_NAME,
            CASH_AMOUNT_FIELD_NAME
        )