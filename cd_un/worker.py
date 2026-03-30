import Pyro5.api
import sys
import redis

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

if __name__ == "__main__":
    
    if len(sys.argv) != 2:
        print(f"Usage: python3 {sys.argv[0]} <port>")
        sys.exit(1)

    port    = int(sys.argv[1])
    
    daemon  = Pyro5.api.Daemon(port=port)

    uri     = daemon.register(Worker(), objectId="worker")

    print("[\033[32m+\033[0m] - Worker running...")
    print("[\033[32m+\033[0m] - NS Entry :", "None")
    print("[\033[32m+\033[0m] -      URI :", uri)

    daemon.requestLoop()