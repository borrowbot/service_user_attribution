from service_user_attribution.src.wrapper import UserAttribution


class ParseUserAttribute(object):
    def __init__(self, sql_params):
        self.sql_params = sql_params
        self.user_table = {}
        self.update_user_table()


    def update_user_table(self):
        self.user_table = {}


    def get_type(self, text):
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


    def get_users(self, text):
        """ Returns a list of found user attributions for a given submission. This works by searching for a 'u/'
            indicator not proceeded by a alphanumeric character, then consuming all the valid reddit username charaters
            following it (letters, numbers, dashes, and underscores). These usernames are then matched with the list of
            known usernames with matching ones being returned.
        """
        # We add a space to the start of the so that a marker u/ at the start of the string will stil lbe triggered
        text = ' ' + text
        maybe_users = re.findall("[^a-z0-9]u/[-_a-z0-9]+", text)
        maybe_users = [u.split('u/')[1] for u in maybe_users]

        if not all([u in self.user_table for u in maybe_users]):
            self.update_user_table()
            maybe_users = [u for u in maybe_users if u in self.user_table]

        return set([self.user_table[u] for u in maybe_users])


    def parse_user_attributions(self, submission):
        parse_title = submission.title.lower()
        attributions = []

        type = get_submission_type(parse_title)
        users = get_submission_users(parse_title)
        source_user = submission.author_id
        try:
            users.remove(source_user)
        except KeyError:
            pass

        ret = [UserAttribution(
            user_id=source_user, resource_id=submission.submission_id,
            type=type, soruce=True
        )]
        for u in users:
            ret.append(UserAttribution(
                user_id=u, resource_id=submission.submission_id,
                type=type, soruce=False
            ))
        return ret
