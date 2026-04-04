import redis

#config
REDIS_HOST = "localhost"
REDIS_PORT = 6379

def main():
    try:
        #connexió a Redis
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        
        #eliminem l'estructura de dades que conté els seients venuts
        r.delete("seients_venuts")
        
        print("Base de dades reiniciada correctament. Tots els seients tornen a estar lliures.")
    except Exception as e:
        print(f"Error connectant a Redis: {e}")

if __name__ == "__main__":
    main()