import Pyro5.api
import itertools
import threading
import sys

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

if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(f"Usage: python3 {sys.argv[0]} <port>")
        sys.exit(1)

    port    = int(sys.argv[1])
    lb_name = "ticket.server.unnumbered"
    
    daemon  = Pyro5.api.Daemon(port=port)
    ns      = Pyro5.api.locate_ns()

    uri     = daemon.register(LoadBalancer(), objectId="loadbalancer")
    ns.register(lb_name, uri)

    print("[\033[32m+\033[0m] - LoadBalancer running...")
    print("[\033[32m+\033[0m] - NS Entry :", lb_name)
    print("[\033[32m+\033[0m] -      URI :", uri)

    daemon.requestLoop()