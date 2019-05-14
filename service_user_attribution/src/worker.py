from sqlalchemy import create_engine


class UserAttributionWorker(object):
    def __init__(self, logger, sql_params):
        self.logger = logger
        self.sql_params = sql_params

    def main(self, block):
        # Create SQL resources
        engine = create_engine("mysql://{}:{}@{}/{}?charaset=utf8m4".format(
            self.sql_params['user'],
            self.sql_params['passwd'],
            self.sql_params['host'],
            self.sql_params['db']
        ), convert_unicode=True, encoding='utf-8')
        con = engine.connect()

        # Oull sumissions for block
        query = '''
            SELECT * FROM submissions WHERE creation_datetime < "{}" AND creation_datetime > "{}"
        '''.format(datetime.utcfromtimestamp(block['end']), datetime.utcfromtimestamp(block['start']))
        submissions = pd.read_sql(sql=query, con=con)
        con.close()
        submissions = [Submission(init_obbject=s[1]) for s in submissions.iterrows()]

        # Pull comments for the block
        bulk_retrieve_comments(submissions, self.sql_params)
