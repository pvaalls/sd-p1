import redis

r = redis.Redis(host='IP_REDIS', port=6379, decode_responses=True)
pubsub = r.pubsub()
pubsub.subscribe("canal_inicio")

print("Esperando señal...")

for message in pubsub.listen():
    if message["type"] == "message":
        if message["data"] == "START":
            pass
