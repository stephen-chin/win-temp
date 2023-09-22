# -*- coding: utf-8 -*- 한글 인코딩 에러를 방지하기 위한 부분
import matplotlib.dates


class Dates:
    def __init__(self):
        pass

    def convert_to_day(self, pillar):
        # calendar = {'D': 1, 'W': 5, 'M': 22, 'Q': 65, 'Y': 261}
        calendar = {'D': 1, 'W': 5, 'M': 20, 'Q': 60, 'Y': 240}
        calendar_figure = int(pillar[:len(pillar) - 1])
        calendar_code = calendar[pillar[-1].upper()]

        return calendar_figure * calendar_code

    def set_major_locator(self, days):
        if days > 261:
            major = matplotlib.dates.YearLocator()
        elif days > 65:
            major = matplotlib.dates.MonthLocator()
        else:
            major = matplotlib.dates.WeekdayLocator()
            # major = matplotlib.dates.DayLocator()

        return major

    def set_minor_locator(self, days):
        if days > 261:
            minor = matplotlib.dates.MonthLocator()
        elif days > 65:
            minor = matplotlib.dates.WeekdayLocator()
        else:
            minor = matplotlib.dates.DayLocator()
        
        return minor

    def set_date_format(self, days):
        if days > 261:
            date_format = matplotlib.dates.DateFormatter('%Y')
        elif days > 63:
            date_format = matplotlib.dates.DateFormatter('%b %y')
        else:
            date_format = matplotlib.dates.DateFormatter('%d %b')

        return date_format