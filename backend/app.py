from flask import Flask, jsonify
import os
import redis

app = Flask(__name__)

redis_host = os.getenv("REDIS_HOST", "redis-svc")
redis_port = int(os.getenv("REDIS_PORT", "6379"))
redis_password = os.getenv("REDIS_PASSWORD") or None

def get_redis():
    return redis.Redis(
        host=redis_host,
        port=redis_port,
        password=redis_password,
        decode_responses=True,
    )

@app.route("/api/ping")
def ping():
    return jsonify({"status": "ok"})

@app.route("/api/count")
def count():
    r = get_redis()
    value = r.incr("visit_count")
    return jsonify({"count": value})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
