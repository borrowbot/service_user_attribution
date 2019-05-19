import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import re

from service_user_attribution.src.wrapper import UserAttribution
from service_user_attribution.src.writer import UserAttributionWriter
from lib_borrowbot_core.raw_objects.submission import bulk_retrieve_comments, Submission


class UserAttributionWorker(object):
    def __init__(self, logger, sql_params, blacklist):
        self.logger = logger
        self.sql_params = sql_params
        self.blacklist = blacklist
        self.sql_writer = UserAttributionWriter(self.logger, self.sql_params, batch_size=float('inf'))


    def update_user_table(self):
        engine = create_engine("mysql://{}:{}@{}/{}?charset=utf8mb4".format(
            self.sql_params['user'],
            self.sql_params['passwd'],
            self.sql_params['host'],
            self.sql_params['db']
        ), convert_unicode=True, encoding='utf-8')
        con = engine.connect()
        query = 'SELECT * FROM user_lookup'
        lookup_table = pd.read_sql(sql=query, con=con)
        con.close()

        user_table = {}
        for r in lookup_table.iterrows():
            user_table[r[1]['user_name'].lower()] = r[1]['user_id']
        return user_table


    def get_submissions(self, block):
        engine = create_engine("mysql://{}:{}@{}/{}?charset=utf8mb4".format(
            self.sql_params['user'],
            self.sql_params['passwd'],
            self.sql_params['host'],
            self.sql_params['db']
        ), convert_unicode=True, encoding='utf-8')
        con = engine.connect()

        query = '''
            SELECT * FROM submissions WHERE creation_datetime < "{}" AND creation_datetime > "{}"
        '''.format(datetime.utcfromtimestamp(block['end']), datetime.utcfromtimestamp(block['start']))
        submissions = pd.read_sql(sql=query, con=con)
        con.close()

        submissions = [Submission(init_object=s[1]) for s in submissions.iterrows()]
        bulk_retrieve_comments(submissions, self.sql_params)
        return submissions


    def get_submission_type(self, text):
        """ Returns one of 'other', 'unpaid', 'paid', or 'request' indicating the type of a resource.
        """
        type = "other"
        if '[req]' in text:
            type = "request"
        elif '[unpaid]' in text:
            type = 'unpaid'
        elif '[paid]' in text:
            type = 'paid'
        return type


    def get_comment_type(self, text):
        type = "other"
        if "$loan" in text:
            type = "loan"
        elif "$confirm" in text:
            type = "confirmation"
        elif "$unpaid" in text:
            type = "unpaid"
        elif "$paid" in text:
            type = "paid"
        return type


    def get_users(self, text, user_table):
        """ Returns a list of found user attributions for a given submission. This works by searching for a 'u/'
            indicator not proceeded by a alphanumeric character, then consuming all the valid reddit username charaters
            following it (letters, numbers, dashes, and underscores). These usernames are then matched with the list of
            known usernames with matching ones being returned.
        """
        # We add a space to the start of the so that a marker u/ at the start of the string will stil lbe triggered
        text = ' ' + text
        maybe_users = re.findall("[^a-z0-9]u/[-_a-z0-9]+", text)
        maybe_users = [u.split('u/')[1] for u in maybe_users]
        maybe_users = [u for u in maybe_users if u in user_table]
        return set([user_table[u] for u in maybe_users])


    def get_users_and_add_attribution(self, ret, type, users, resource_id, source_user, submission_dt):
        if source_user in users:
            users.remove(source_user)
        if source_user is not None:
            ret.append(UserAttribution(
                user_id=source_user, resource_id=resource_id, type=type,
                source=True, submission_datetime=submission_dt
            ))
        for u in users:
            ret.append(UserAttribution(
                user_id=u, resource_id=resource_id, type=type,
                source=False, submission_datetime=submission_dt
            ))
        return ret


    def parse_user_attributions(self, submission, user_table):
        attributions = []
        ret = []

        # Parse submission
        parse_title = submission.title.lower()
        type = self.get_submission_type(parse_title)
        users = self.get_users(parse_title, user_table)
        source_user = submission.author_id
        self.get_users_and_add_attribution(
            ret, type, users, submission.submission_id,
            source_user, submission.creation_datetime
        )

        # Parse submission comments
        for c in submission.comments:
            if c.author_id in self.blacklist:
                continue
            comment_text= c.text.lower()
            type = self.get_comment_type(comment_text)
            users = self.get_users(comment_text, user_table)
            if type == "loan" and submission.author_id is not None:
                users.add(submission.author_id)
            source_user = c.author_id
            self.get_users_and_add_attribution(
                ret, type, users, c.comment_id,
                source_user, submission.creation_datetime
            )

        return ret


    def main(self, block):
        submissions = self.get_submissions(block)
        user_lookup_table = self.update_user_table()

        for s in submissions:
            user_attributions = self.parse_user_attributions(s, user_lookup_table)
            for u in user_attributions:
                self.sql_writer.push(u)
        self.sql_writer.flush()
