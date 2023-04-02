import pandas as pd
from datetime import datetime, timedelta
import pytz
from tqdm import tqdm

store_status = pd.read_csv("cache/store status.csv")
menu_hours = pd.read_csv("cache/Menu hours.csv")
store_timezones = pd.read_csv("cache/store timezones.csv")

time_format1 = "%Y-%m-%d %H:%M:%S %Z"
time_format2 = "%Y-%m-%d %H:%M:%S.%f %Z"


def get_all_status_by_store_id(store_id):
    """
    gets store_id and returns all recorded data for that store_id from store_status ordered by date.
    also converts timezones from utc to stores timezone in store_timezones.
    :param store_id:
    :return: return an ordered list that each element is in this format: (date, status)
    """
    try:
        local_timezone = store_timezones[store_timezones.store_id == store_id].iloc[
            0, 1
        ]
    except:
        local_timezone = "America/Chicago"
    l = []
    for row in store_status[store_status.store_id == store_id].itertuples(index=False):
        try:
            dt = datetime.strptime(row.timestamp_utc, time_format1)
        except:
            dt = datetime.strptime(row.timestamp_utc, time_format2)
        # Create a time zone object
        timezone = pytz.timezone("UTC")

        # Convert the datetime object to the desired time zone
        dt_with_timezone = timezone.localize(dt)

        new_timezone = pytz.timezone(local_timezone)
        converted_time = dt_with_timezone.astimezone(new_timezone)
        l.append((converted_time, row.status))

    sorted_list = sorted(l, key=lambda x: x[0])
    return sorted_list


def status_at_date(date, sorted_):
    """
    return data for a store in specified date.
    :param date: a datetime object
    :param sorted_:  a sorted list with each element in this format: (date, status)
    :return: return all data in specified date
    """
    res = []
    for el in sorted_:
        if (
            el[0].day == date.day
            and el[0].year == date.year
            and el[0].month == date.month
        ):
            res.append(el)
    return res


def compute_status_by_day(x, store_id):
    """
    this function is where the main logic has implemented. x is a sorted list in a specific
    day in this format: (date, status). first we get working hours of store with store_id,
    then we group every data in x according to which it belongs. then we mark that interval according
    to status of data. here is an example: suppose store has two data intervals: 10 -> 12 and 14 -> 22,
    and we have this data points: (11:14, active), (11:54, inactive), (12:34, inactive), (15:45, active),
    (17:50, active). first of all (12:34, inactive) will not be in our calculations because it's not int the
    working hours of store. first two point will be in 10 -> 12 group and last two points in the 14 -> 22
    group. then we fill the active and inactive according to data. like from 10 to 11:14 will be active,
    from 11:14 to 11:54 also active,but from 11:54 to 12 will be inactive.
    :param x: data in a day
    :param store_id: id of the store
    :return: return amount of uptime and downtime in specified day.
    """
    if x is None or len(x) == 0:
        return 0, 0
    weekday = x[0][0].weekday()
    day = x[0][0].day
    month = x[0][0].month
    year = x[0][0].year
    timezone = x[0][0].tzinfo

    open_intervals = []
    if (
        len(
            menu_hours[
                (menu_hours.store_id == store_id) & (menu_hours.day == weekday)
            ].value_counts()
        )
        == 0
    ):
        start = datetime.strptime("00:00:00", "%H:%M:%S")
        start = start.replace(year=year, month=month, day=day)
        start = timezone.localize(start)
        end = datetime.strptime("23:59:59", "%H:%M:%S")
        end = end.replace(year=year, month=month, day=day)
        end = timezone.localize(end)
        open_intervals.append((start, end))
    else:
        for row in menu_hours[
            (menu_hours.store_id == store_id) & (menu_hours.day == weekday)
        ].itertuples(index=False):
            start = datetime.strptime(row.start_time_local, "%H:%M:%S")
            start = start.replace(year=year, month=month, day=day)
            start = timezone.localize(start)
            end = datetime.strptime(row.end_time_local, "%H:%M:%S")
            end = end.replace(year=year, month=month, day=day)
            end = timezone.localize(end)
            open_intervals.append((start, end))

    dic = {}
    for el in x:
        for intr in open_intervals:
            if intr[0] < el[0] < intr[1]:
                dic.setdefault(intr, []).append(el)

    active = 0
    inactive = 0
    for key, value in dic.items():
        delta = value[0][0] - key[0]
        delta = delta.total_seconds() / 60
        if value[0][1] == "active":
            active += delta
        else:
            inactive += delta

        for i in range(len(value) - 1):
            delta = value[i + 1][0] - value[i][0]
            delta = delta.total_seconds() / 60
            if value[i][1] == "active":
                active += delta
            else:
                inactive += delta
        delta = key[1] - value[-1][0]
        delta = delta.total_seconds() / 60
        if value[-1][1] == "active":
            active += delta
        else:
            inactive += delta

    return active, inactive


def compute_status_week(x, store_id, today):
    """
    computes active and inactive time starting from a giving date using compute_status_by_day
    function.
    :param x: data
    :param store_id: id of store
    :param today: a datetime object for a day
    :return: return uptime and downtime for starting date a last week.
    """
    active = 0
    inactive = 0
    l = get_all_status_by_store_id(store_id)
    counter = 6
    while not x and counter > 0:
        today = today - timedelta(days=1)
        x = status_at_date(today, l)
        counter -= 1
    if not x:
        return 0, 0, 0, 0
    a, i = compute_status_by_day(x, store_id)
    active += a
    inactive += i
    day_active = active
    day_inactive = inactive
    for i in range(6):
        x_ = x[0][0] - timedelta(days=i)
        x__ = status_at_date(x_, l)
        a, i = compute_status_by_day(x__, store_id)
        active += a
        inactive += i

    return active, inactive, day_active, day_inactive


def get_data_hour(date, status):
    """
    return data in an hour
    :param date: datetime object
    :param status:
    :return:
    """
    last_hour = date - timedelta(hours=1)
    for s in status:
        if s[0].year == date.year and s[0].month == date.month and s[0].day == date.day:
            if last_hour.hour <= s[0].hour <= date.hour:
                return s


def last_hour(x):
    if not x:
        return 0, 0
    if x[1] == "active":
        return 60, 0
    else:
        return 0, 60


def compute_week_day_for_all(
    store_status, current_date=datetime(2023, 1, 25, 19, 0, 0)
):
    """
    computes all uptime and downtime data for all of stores caches in cache folder.
    :param store_status: pandas dataframe for status data
    :param current_date: self-explanatory
    :return: returns a dict with format: {store_id: (uptime_week, downtime_week, uptime_day,
    downtime_day, uptime_hour, downtime_hour), ...}
    """
    info = {}
    for id in tqdm(store_status.store_id.unique()):
        l = get_all_status_by_store_id(id)
        x = status_at_date(current_date, l)
        x_h = get_data_hour(current_date, l)
        a_h, i_h = last_hour(x_h)
        a, i, a_d, i_d = compute_status_week(x, id, current_date)
        info[id] = (a, i, a_d, i_d, a_h, i_h)
    return info


def to_csv(data_dict, path):
    """
    saves and caches computed data to cache folder.
    :param data_dict: return value of compute_week_day_for_all function
    :param path: cache path
    :return: None
    """
    df = pd.DataFrame.from_dict(
        data_dict,
        orient="index",
        columns=[
            "uptime_lastweek",
            "downtime_lastweek",
            "uptime_lastday",
            "downtime_lastday",
            "'uptime_lasthour",
            "downtime_lasthour",
        ],
    )
    df.index.name = "store_id"
    df.reset_index(inplace=True)
    df.to_csv(path, index=False)
