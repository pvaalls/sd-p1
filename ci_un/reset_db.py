import redis

#per ficar la bd a 0 per si parem l'execucio a la mitat

#config
REDIS_HOST = "localhost"
REDIS_PORT = 6379

def main():
    try:
        #conec a redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        
        #fiquem el contador a 0
        r.set("entrades_venudes", 0)
        
        print("Base de dades reiniciada correctament. 'entrades_venudes' = 0.")
    except Exception as e:
        print(f"Error connectant a Redis: {e}")

if __name__ == "__main__":
    main()