from zipline.pipeline.loaders.events import EventsLoader
from zipline.pipeline.data.dividends import CashDividends
from zipline.utils.memoize import lazyval


class CashDividendsLoader(EventsLoader):
    expected_cols = frozenset([EX_DATE_FIELD_NAME,
                               PAY_DATE_FIELD_NAME,
                               RECORD_DATE_FIELD_NAME,
                               CASH_AMOUNT_FIELD_NAME])

    def __init__(self, all_dates, events_by_sid,
                 infer_timestamps=False,
                 dataset=CashDividends):
        super(CashDividendsLoader, self).__init__(
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
    def next_record_date_loader(self):
        return self._next_event_date_loader(self.dataset.next_record_date,
                                            RECORD_DATE_FIELD_NAME)

    @lazyval
    def previous_record_date_loader(self):
        return self._previous_event_date_loader(
            self.dataset.previous_record_date,
            RECORD_DATE_FIELD_NAME
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
