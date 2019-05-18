class UserAttribution(object):
    def __init__(self, user_id, resource_id, type, source, submission_datetime):
        self.compound_key = "{}+{}".format(user_id, resource_id)
        self.user_id = user_id
        self.resource_id = resource_id
        self.type = type
        self.source = source
        self.submission_datetime = submission_datetime
