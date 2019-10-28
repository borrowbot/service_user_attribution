import json
import threading
from flask import request

from baseimage.config import CONFIG
from baseimage.flask import get_flask_server
from baseimage.logger.logger import get_default_logger

from service_user_attribution.src.worker import UserAttributionWorker
from service_user_attribution.src.block_generator import UserAttributionBlockGenerator

from lib_learning.collection.scheduler import Scheduler
from lib_learning.collection.interfaces.local_interface import LocalInterface


# interface
interface = LocalInterface()

# workers
worker_logger = get_default_logger('worker')
worker = UserAttributionWorker(interface, worker_logger, CONFIG['sql'], CONFIG['blacklist'])
worker.start()

# schedulers
scheduler_logger = get_default_logger('scheduler')
block_generator = UserAttributionBlockGenerator(CONFIG['sql'])
scheduler = Scheduler(
    'service_submission_parser', interface, block_generator, scheduler_logger,
    task_timeout=600, confirm_interval=10
)

# service server
# TODO: endpoints here should be standardized and moved into lib_learning.collection
server = get_flask_server()


@server.route('/push', methods=["POST"])
def push():
    days = request.args.get('days', type=int, default=1)
    return json.dumps(scheduler.push_next_block(days=days))


@server.route('/get_queue')
def get_queue():
    return json.dumps(scheduler.pending_work)


if __name__ == "__main__":
    server.run(port=CONFIG['port'])
