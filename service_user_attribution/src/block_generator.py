import MySQLdb as sql
import calendar
from datetime import datetime
from datetime import timedelta

from lib_learning.collection.base_generator import WorkBlockGenerator


# 2016-01-01 00:00:00 UTC
DEFAULT_START_DATE = datetime.utcfromtimestamp(1496275200)


class UserAttributionBlockGenerator(WorkBlockGenerator):
    def __init__(self, sql_params, default_start_date=DEFAULT_START_DATE):
        self.sql_params = sql_params
        self.default_start_date = default_start_date


    def get_next(self, days=10):
        start_submission_date = self.get_newest_parsed_submission()
        end_submission_date = start_submission_date + timedelta(days=days)
        return {
            'start': calendar.timegm(start_submission_date.timetuple()),
            'end': calendar.timegm(end_submission_date.timetuple())
        }


    def get_first_submission_date(self):
        query = 'SELECT MIN(creation_datetime) as min FROM submissions WHERE subreddit_name="borrow";'

        db = sql.connect(**self.sql_params)
        cur = db.cursor()
        cur.execute(query)
        db.commit()
        result_set = cur.fetchall()
        cur.close()
        db.close()

        last_result = result_set[0][0]
        if last_result is None:
            raise Exception("Attempting to start submission parser without any submissions")
        else:
            return last_result


    def get_newest_parsed_submission(self):
        query = 'SELECT MAX(req_datetime) as max FROM requests;'

        db = sql.connect(**self.sql_params)
        cur = db.cursor()
        cur.execute(query)
        db.commit()
        result_set = cur.fetchall()
        cur.close()
        db.close()

        last_result = result_set[0][0]
        if last_result is None and self.default_start_date is None:
            return self.get_first_submission_date() - timedelta(seconds=5)
        elif last_result is None:
            return self.default_start_date
        else:
            return last_result
