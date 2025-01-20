import os
import logging
import sys
import redis
import time
import subprocess
import select

# Configure logging to write to stdout
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger(__name__)

logger.info("Worker starting...")  # This should show up immediately


def run_worker():
    logger.info("Worker function starting...")
    redis_client = redis.Redis(
        host='redis',
        port=6379,
        decode_responses=True
    )

    # Test Redis connection
    logger.info(f"Redis connection test: {redis_client.ping()}")  # Add this

    while True:
        logger.info("Waiting on new script...")
        _, script_name = redis_client.brpop(["script_queue"])
        logger.info(f"Received script: {script_name}")
        try:
            # Print script path for debugging
            script_path = f"/app/scraper/scripts/{script_name}"
            logger.info(f"Attempting to run script at: {script_path}")

            # Check if file exists
            if not os.path.exists(script_path):
                print(f"Error: Script not found at {script_path}")
                redis_client.set(f"result:{script_name}", "Error: Script not found")
            else:
                process = subprocess.Popen(
                    ["python", script_path],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    universal_newlines=True
                )

                # Use select for non-blocking IO
                outputs = [process.stdout, process.stderr]
                while outputs:
                    readable, _, _ = select.select(outputs, [], [])
                    for output in readable:
                        line = output.readline()
                        if line:
                            line = line.strip()
                            redis_client.lpush(f"output:{script_name}", line)
                        else:
                            outputs.remove(output)

                # Wait and get return code
                return_code = process.wait()
                logger.info(f"Process completed with return code: {return_code}")

                # set the entire output as a result
                all_output = redis_client.lrange(f"output:{script_name}", 0, -1)
                all_output.reverse()
                final_result = "\n".join(all_output)
                redis_client.set(f"result:{script_name}", final_result)

        except Exception as e:
            print(f"Exception occurred: {str(e)}")
            redis_client.set(f"result:{script_name}", f"Error: {str(e)}")


if __name__ == "__main__":
    logger.info("Main block starting...")
    run_worker()
