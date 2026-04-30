import redis

r = redis.Redis(host='IP_REDIS', port=6379, decode_responses=True)

print("Enviando señal START...")
r.publish("canal_inicio", "START")
