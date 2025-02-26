import datetime


def bin_date_range(start_date: datetime.datetime,
                   end_date: datetime.datetime,
                   size: int = 5
                   ) -> list[datetime.datetime] | None:
    if end_date is None or start_date is None or end_date < start_date or end_date.year == 9999:
        return []
    # create bins
    first_day = datetime.datetime(start_date.year, 1, 1)
    all_five_day_bins = []
    next_day = first_day
    changed_year_flag = False
    while next_day < end_date:
        if (next_day.year != first_day.year) and (not changed_year_flag):
            next_day = datetime.datetime(start_date.year + 1, 1, 1)
            changed_year_flag = True
        all_five_day_bins.append(next_day)
        next_day += datetime.timedelta(days=size)
        if next_day > end_date:
            all_five_day_bins.append(end_date)

    binned_range = [start_date]
    for i in sorted(set(all_five_day_bins)):
        if start_date < i < end_date:
            binned_range.append(i)
        elif i > end_date:
            break
    binned_range.append(end_date)
    binned_range = sorted(list(set(binned_range)))
    return binned_range


def bin_and_tuple_date_range(
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        size: int = 5) -> list[tuple[str, str]]:
    date_ranges: list[datetime.datetime] = sorted(bin_date_range(start_date, end_date, size), reverse=True)
    date_ranges: list[str] = [date_range.strftime('%Y-%m-%d') for date_range in date_ranges]
    date_ranges: list[tuple[str, str]] = [
        (date_ranges[c + 1], i) for c, i in enumerate(date_ranges) if c != len(date_ranges) - 1
    ]
    return date_ranges
