from lib_learning.collection.batch_writer import BatchWriter


class UserAttributionWriter(BatchWriter):
    def __init__(self, logger, sql_parameters, batch_size=16):
        template = {
            'compound_key': 'compound_key',
            "submission_datetime": 'submission_datetime',
            'user_id': 'user_id',
            'resource_id': 'resource_id',
            'type': 'type',
            'source': 'source'
        }
        table_name = 'user_attribution'
        super().__init__(logger, template, table_name, sql_parameters, batch_size)
