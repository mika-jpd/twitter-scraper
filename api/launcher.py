import json
import uuid
from datetime import datetime

from fastapi import FastAPI
import redis

app = FastAPI()
redis_client = redis.Redis(
    host='redis',
    port=6379,
    decode_responses=True
)


@app.post("/run-script/{name}")
def run_script(name: str):
    # Push script name to a queue
    redis_client.lpush("script_queue", name)
    return {"message": f"Script {name} queued"}


@app.get("/check-results/{name}")
def check_results(name: str):
    # Get result if exists
    result = redis_client.get(f"result:{name}")
    if result is None:
        return {"status": "pending", "result": None}
    return {"status": "complete", "result": result}

@app.get("/check-output/{name}")
def check_output(name: str):
    # Get all lines of output so far
    output: list = redis_client.lrange(f"output:{name}", 0, -1)
    # Redis LRANGE returns in reverse order, so we need to reverse it back
    output.reverse()

    # check script status
    result = redis_client.get(f"result:{name}")
    status = "complete" if result else "pending"
    return {"status": status, "output": output}


@app.get("/check-all-scripts")
def check_all_scripts():
    try:
        # Get all queued scripts
        queued_scripts = redis_client.lrange("script_queue", 0, -1)

        # Get all running scripts (those with output but no result)
        # First, get all keys that start with "output:"
        output_keys = redis_client.keys("output:*")
        running_scripts = []

        for key in output_keys:
            script_name = key.replace("output:", "")
            # If it has output but no result, it's still running
            if redis_client.get(f"result:{script_name}") is None:
                running_scripts.append(script_name)

        return {
            "queued": queued_scripts,
            "running": running_scripts,
            "total_queued": len(queued_scripts),
            "total_running": len(running_scripts)
        }

    except redis.RedisError as e:
        return {"status": "error", "message": str(e)}