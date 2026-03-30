import Pyro5.api
import itertools
import threading
import sys

# 🔥 Configuración de concurrencia
Pyro5.config.SERVERTYPE = "thread"
Pyro5.config.THREADPOOL_SIZE = 50
Pyro5.config.COMMTIMEOUT = 2  # evita bloqueos largos

# Workers
worker_uris = [
    "PYRO:worker@localhost:9001",
    "PYRO:worker@localhost:9002",
]

# Round-robin sin lock
counter = itertools.count()

# Thread-local storage
thread_local = threading.local()


def get_proxies():
    """Inicializa proxies por hilo (lazy)"""
    if not hasattr(thread_local, "proxies"):
        thread_local.proxies = {uri: Pyro5.api.Proxy(uri) for uri in worker_uris}
    return thread_local.proxies


def next_worker():
    return worker_uris[next(counter) % len(worker_uris)]


@Pyro5.api.expose
class LoadBalancer:

    def comprar_entrada(self, client_id, request_id):
        print(f"{client_id} -> {request_id}")

        proxies = get_proxies()

        # Intentar con todos los workers (failover)
        for _ in range(len(worker_uris)):
            uri = next_worker()
            worker = proxies[uri]

            try:
                return worker.comprar_entrada(client_id, request_id)

            except Exception as e:
                print(f"[ERROR] Worker {uri} falló: {e}")

        # Si todos fallan
        return False


if __name__ == "__main__":

    if len(sys.argv) != 2:
        print(f"Usage: python3 {sys.argv[0]} <port>")
        sys.exit(1)

    port = int(sys.argv[1])
    lb_name = "ticket.server.unnumbered"

    daemon = Pyro5.api.Daemon(port=port)
    ns = Pyro5.api.locate_ns()

    uri = daemon.register(LoadBalancer(), objectId="loadbalancer")
    ns.register(lb_name, uri)

    print("[\033[32m+\033[0m] - LoadBalancer running...")
    print("[\033[32m+\033[0m] - NS Entry :", lb_name)
    print("[\033[32m+\033[0m] -      URI :", uri)

    daemon.requestLoop()