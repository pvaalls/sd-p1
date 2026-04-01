import Pyro5.api
import itertools
import threading
import sys
import argparse

Pyro5.config.SERVERTYPE = "thread"
Pyro5.config.THREADPOOL_SIZE = 50

# URIs de los workers (objectId = "worker")
worker_uris = [
    "PYRO:worker@localhost:9001",
    "PYRO:worker@localhost:9002",
]

counter = itertools.count()
thread_local = threading.local()

def get_proxy(uri):
    if not hasattr(thread_local, "proxies"):
        # Inicializar proxies por hilo
        thread_local.proxies = {u: Pyro5.api.Proxy(u) for u in worker_uris}
    return thread_local.proxies[uri]

@Pyro5.api.expose
class LoadBalancer:

    def comprar_entrada(self, client_id, request_id):
        print(f"{client_id} -> {request_id}")
        uri = worker_uris[next(counter) % len(worker_uris)]
        try:
            worker = get_proxy(uri)
            return worker.comprar_entrada(client_id, request_id)
        except Exception as e:
            print(e)
            return False

def main():
    
    parser = argparse.ArgumentParser(description="Load Balancer")
    parser.add_argument("-n", "--ns", type=str, default="localhost", help="Specifies to use the given nameserver (default: %(default)s)")
    parser.add_argument("-H", "--host", type=str, default="localhost", help="Specifies to use the given host (default: %(default)s)")
    parser.add_argument("-p", "--port", type=int, default=9000, help="Specifies to use the given port (default: %(default)s)")
    args = parser.parse_args()

    lb_name = "ticket.server.unnumbered"
    
    daemon  = Pyro5.api.Daemon(host=args.host,port=args.port)
    ns      = Pyro5.api.locate_ns(host=args.ns)

    uri     = daemon.register(LoadBalancer(), objectId="loadbalancer")
    ns.register(lb_name, uri)

    print("[\033[32m+\033[0m] - LoadBalancer running...")
    print("[\033[32m+\033[0m] - NS Entry :", f"\033[32m{lb_name}\033[0m")
    print("[\033[32m+\033[0m] -      URI :", f"\033[32m{uri}\033[0m")

    daemon.requestLoop()

if __name__ == "__main__":
    main()