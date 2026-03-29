import Pyro5.api
import sys
import redis

# 🔥 multihilo
Pyro5.api.config.SERVERTYPE = "thread"

@Pyro5.api.expose
class Worker:

    limite_entradas = 20000

    def __init__(self):
        self.redisserver = redis.Redis(
            host="localhost",
            port=6379,
            db=0,
            decode_responses=True
        )

        # Inicializa solo si no existe
        self.redisserver.setnx("entrades_venudes", 0)

    def comprar_entrada(self, client_id, request_id):
        try:
            print(f"{client_id} -> {request_id}")
            entradas = self.redisserver.incr("entrades_venudes")

            if entradas <= self.limite_entradas:
                return True
            else:
                return False

        except Exception as e:
            print("Error en Worker:", e)
            return False


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python worker.py <PUERTO>")
        sys.exit(1)

    port = int(sys.argv[1])

    daemon = Pyro5.api.Daemon(port=port)

    worker_obj = Worker()
    uri = daemon.register(worker_obj, objectId="worker")

    print(f"✅ Worker en puerto {port} → {uri}")

    daemon.requestLoop()