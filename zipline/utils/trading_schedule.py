#
# Copyright 2016 Quantopian, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from abc import (
    ABCMeta,
    abstractmethod,
    abstractproperty,
)
import bisect
from pandas import Timedelta

from zipline.utils.exchange_calendar import normalize_date
from zipline.utils.nyse_exchange_calendar import NYSEExchangeCalendar


class TradingSchedule(object):
    """
    A TradingSchedule defines the execution timing of a TradingAlgorithm.
    """
    __metaclass__ = ABCMeta

    @abstractproperty
    def day(self):
        """
        A CustomBusinessDay defining those days on which the algorithm is
        usually trading.
        """
        raise NotImplementedError()

    @abstractproperty
    def tz(self):
        """
        The native timezone for this TradingSchedule.
        """
        raise NotImplementedError()

    @abstractproperty
    def schedule(self):
        """
        A DataFrame, with a DatetimeIndex of trading dates, containing columns
        of trading starts and ends in this TradingSchedule.
        """
        raise NotImplementedError()

    @abstractproperty
    def first_trading_day(self):
        """
        The first possible day of trading in this TradingSchedule.
        """
        raise NotImplementedError()

    @abstractproperty
    def last_trading_day(self):
        """
        The last possible day of trading in this TradingSchedule.
        """
        raise NotImplementedError()

    @abstractmethod
    def trading_sessions(self, start, end):
        """
        Calculates all of the trading sessions between the given
        start and end.

        Parameters
        ----------
        start : Timestamp
        end : Timestamp

        Returns
        -------
        DataFrame
            A DataFrame, with a DatetimeIndex of trading dates, containing
            columns of trading starts and ends in this TradingSchedule.
        """
        raise NotImplementedError()

    @property
    def all_trading_dates(self):
        return self.schedule.index

    def trading_dates(self, start, end):
        """
        Calculates the dates of all of the trading sessions between the given
        start and end.

        Parameters
        ----------
        start : Timestamp
        end : Timestamp

        Returns
        -------
        DatetimeIndex
            A DatetimeIndex containing the dates of the desired trading
            sessions.
        """
        return self.trading_sessions(start, end).index

    @abstractmethod
    def data_availability_time(self, date):
        """
        Given a UTC-canonicalized date, returns a time by-which all data from
        the previous date is available to the algorithm.

        Parameters
        ----------
        date : Timestamp
            The UTC-canonicalized calendar date whose data availability time
            is needed.

        Returns
        -------
        Timestamp or None
            The data availability time on the given date, or None if there is
            no data availability time for that date.
        """
        raise NotImplementedError()

    @abstractmethod
    def start_and_end(self, date):
        """
        Given a UTC-canonicalized date, returns a tuple of timestamps of the
        start and end of the algorithm trading session for that date.

        Parameters
        ----------
        date : Timestamp
            The UTC-canonicalized algorithm trading session date whose start
            and end are needed.

        Returns
        -------
        (Timestamp, Timestamp)
            The start and end for the given date.
        """
        raise NotImplementedError()

    @abstractmethod
    def is_execution_time(self, dt):
        """
        Calculates if a TradingAlgorithm using this TradingSchedule should be
        executed at time dt.

        Parameters
        ----------
        dt : Timestamp
            The time being queried.

        Returns
        -------
        bool
            True if the TradingAlgorithm should be executed at dt,
            otherwise False.
        """
        raise NotImplementedError()

    @abstractmethod
    def is_execution_date(self, dt):
        """
        Calculates if a TradingAlgorithm using this TradingSchedule would execute.

        Parameters
        ----------
        dt : Timestamp
            The time being queried.

        Returns
        -------
        bool
            True if the TradingAlgorithm should be executed at dt,
            otherwise False.
        """
        raise NotImplementedError()

    @abstractmethod
    def minute_window(self, start, count, step=1):
        """
        Return a DatetimeIndex containing `count` market minutes, starting with
        `start` and continuing `step` minutes at a time.

        Parameters
        ----------
        start : Timestamp
            The start of the window.
        count : int
            The number of minutes needed.
        step : int
            The step size by which to increment.

        Returns
        -------
        DatetimeIndex
            A window with @count minutes, starting with @start a returning
            every @step minute.
        """
        raise NotImplementedError()

    def next_trading_day(self, test_date):
        dt = normalize_date(test_date)
        delta = Timedelta(days=1)

        while dt <= self.last_trading_day:
            dt += delta
            if self.is_execution_date(dt):
                return dt
        return None

    def trading_day_distance(self, first_date, second_date):
        first_date = normalize_date(first_date)
        second_date = normalize_date(second_date)
        all_days = self.schedule.index

        i = bisect.bisect_left(all_days, first_date)
        if i == len(all_days):  # nothing found
            return None
        j = bisect.bisect_left(all_days, second_date)
        if j == len(all_days):
            return None
        distance = j - 1
        assert distance >= 0
        return distance


class ExchangeTradingSchedule(TradingSchedule):
    """
    A TradingSchedule that functions as a wrapper around an ExchangeCalendar.
    """

    def __init__(self, cal):
        """
        Docstring goes here, Jimmy

        Parameters
        ----------
        cal : ExchangeCalendar
            The ExchangeCalendar to be represented by this
            ExchangeTradingSchedule.
        """
        self._exchange_calendar = cal

    @property
    def day(self):
        return self._exchange_calendar.day

    @property
    def tz(self):
        return self._exchange_calendar.tz

    @property
    def schedule(self):
        return self._exchange_calendar.schedule

    @property
    def first_trading_day(self):
        return self._exchange_calendar.first_trading_day

    @property
    def last_trading_day(self):
        return self._exchange_calendar.last_trading_day

    def trading_sessions(self, start, end):
        """
        See TradingSchedule definition.
        """
        return self._exchange_calendar.trading_days(start, end)

    def data_availability_time(self, date):
        """
        See TradingSchedule definition.
        """
        calendar_open, _ = self._exchange_calendar.open_and_close(date)
        return calendar_open

    def start_and_end(self, date):
        """
        See TradingSchedule definition.
        """
        return self._exchange_calendar.open_and_close(date)

    def is_execution_time(self, dt):
        """
        See TradingSchedule definition.
        """
        return self._exchange_calendar.is_open_on_minute(dt)

    def is_execution_date(self, dt):
        """
        See TradingSchedule definition.
        """
        return self._exchange_calendar.is_open_on_date(dt)

    def minute_window(self, start, count, step=1):
        return self._exchange_calendar.minute_window(start=start,
                                                     count=count,
                                                     step=step)


class NYSETradingSchedule(ExchangeTradingSchedule):
    """
    An ExchangeTradingSchedule for NYSE. Provided for convenience.
    """
    def __init__(self):
        super(NYSETradingSchedule, self).__init__(cal=NYSEExchangeCalendar())

default_nyse_schedule = NYSETradingSchedule()