import Pyro5.api
import sys
import redis
import argparse

@Pyro5.api.expose
class Worker:

    limite_entradas = 20000

    def __init__(self):
        # Conexión a Redis
        self.redisserver = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
        # Solo inicializa si no existe
        self.redisserver.setnx("entrades_venudes", 0)

    def comprar_entrada(self, client_id, request_id):
        print(f"{client_id} -> {request_id}")
        try:
            entradas = self.redisserver.incr("entrades_venudes")
            return entradas <= self.limite_entradas
        except Exception as e:
            print("Error en Worker:", e)
            return False

def main():
    
    parser = argparse.ArgumentParser(description="Load Balancer")
    parser.add_argument("-p", "--port", type=int, required=True, help="Specifies to use the given port")
    args = parser.parse_args()

    daemon = Pyro5.api.Daemon(host="localhost",port=args.port)

    uri    = daemon.register(Worker(), objectId="worker")

    print("[\033[32m+\033[0m] - Worker running...")
    print("[\033[32m+\033[0m] - NS Entry :", "\033[32mNone\033[0m")
    print("[\033[32m+\033[0m] -      URI :", f"\033[32m{uri}\033[0m")

    daemon.requestLoop()

if __name__ == "__main__":
    main()