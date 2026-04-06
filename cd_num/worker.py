import Pyro5.api
import sys
import signal
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

def register_to_lb(uri, ns_host, lb_ns_entry="ticket.server.unnumbered"):
    try:
        ns = Pyro5.api.locate_ns(host=ns_host)
        lb_uri = ns.lookup(lb_ns_entry)
        lb = Pyro5.api.Proxy(lb_uri)

        lb.register_worker(uri)
        print("[\033[32m+\033[0m] -   Status :", "\033[32mRegistered on LB\033[0m")

        return lb
    except Exception as e:
        print("[\033[31m+\033[0m] -   Status :", "\033[31mNot registered on LB\033[0m")
        return None

def main():
    
    parser = argparse.ArgumentParser(description="Worker")
    parser.add_argument("-p", "--port", type=int, required=True, help="Specifies to use the given port")
    parser.add_argument("-n", "--ns", type=str, default="localhost", help="Specifies to use the given nameserver (default: %(default)s)")
    args = parser.parse_args()

    daemon = Pyro5.api.Daemon(host="localhost",port=args.port)

    uri    = daemon.register(Worker(), objectId="worker")

    print("[\033[32m+\033[0m] - Worker running...")
    print("[\033[32m+\033[0m] - NS Entry :", "\033[32mNone\033[0m")
    print("[\033[32m+\033[0m] -      URI :", f"\033[32m{uri}\033[0m")

    lb = register_to_lb(uri, args.ns)
    if lb is None:
        sys.exit(1)
    def shutdown(signum, frame):
        print("[\033[33m!\033[0m] - Aborting...")

        try:
            if lb:
                lb.unregister_worker(uri)
        except Exception as e:
            print("Error al desregistrar:", e)

        #daemon.shutdown()
        sys.exit(1)

    signal.signal(signal.SIGINT, shutdown)

    daemon.requestLoop()

if __name__ == "__main__":
    main()