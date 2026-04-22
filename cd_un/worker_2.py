import Pyro5.api
import sys
import signal
import redis
import argparse

@Pyro5.api.expose
class Worker:

    limite_entradas = 20000

    def __init__(self, verbose, redis_host):
        self.vprint = print if verbose else lambda *args, **kwargs: None
        # Conexión a Redis
        self.redis_server = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)
        # Solo inicializa si no existe
        self.redis_server.setnx("entradas_vendidas_un", 0)

    def comprar_entrada(self, client_id, request_id):
        self.vprint(f"{client_id} -> {request_id}")
        try:
            entradas = self.redis_server.incr("entradas_vendidas_un")
            return entradas <= self.limite_entradas
        except Exception as e:
            print("Error en Worker:", e)
            return False

def register_to_lb(uri, ns_host, lb_ns_entry="ticket.server.unnumbered"):
    try:
        ns     = Pyro5.api.locate_ns(host=ns_host)
        lb_uri = ns.lookup(lb_ns_entry)
        lb     = Pyro5.api.Proxy(lb_uri)

        lb.register_worker(uri)
        print("[\033[32m+\033[0m] -   Status :", "\033[32mRegistered on LB\033[0m")

        return lb
    except Exception as e:
        print("[\033[31m+\033[0m] -   Status :", "\033[31mNot registered on LB\033[0m")
        return None

def main():

    parser = argparse.ArgumentParser(description="Worker (Unnumbered Tickets)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Specifies to use the verbose mode")
    parser.add_argument("-n", "--ns", type=str, default="localhost", help="Nameserver (default: %(default)s)")
    parser.add_argument("-H", "--host", type=str, default="localhost", help="Host (default: %(default)s)")
    parser.add_argument("-R", "--redis", type=str, default="localhost", help="Redis Host (default: %(default)s)")
    parser.add_argument("-p", "--port", type=int, required=True, help="Port")
    args = parser.parse_args()

    daemon = Pyro5.api.Daemon(host=args.host,port=args.port)

    uri    = daemon.register(Worker(args.verbose,args.redis), objectId="worker")

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

        sys.exit(1)

    signal.signal(signal.SIGINT, shutdown)

    daemon.requestLoop()

if __name__ == "__main__":
    main()
