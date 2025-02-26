from redis import Redis
from rq import Queue
from rq_scheduler import Scheduler
import platform

if platform.system() == "Darwin":
    host = "localhost"
elif platform.system() == "Linux":
    host = "redis"
else:
    raise Exception(f"Unsupported platform {platform.system()}")

redis_client = Redis(
    host=host,
    port=6379,
    #decode_responses=True
)

# Define different queues for different tasks
scraper_queue = Queue("scraper_jobs", connection=redis_client)
account_queue = Queue("account_jobs", connection=redis_client)

# define a scheduler for the scraper_jobs
scheduler = Scheduler(queue=scraper_queue, connection=scraper_queue.connection)
