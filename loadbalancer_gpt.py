import Pyro5.api
import itertools
import threading

# 🔥 IMPORTANTE: servidor multihilo
Pyro5.api.config.SERVERTYPE = "thread"

worker_uris = [
    "PYRO:worker@localhost:9001",
    "PYRO:worker@localhost:9002",
]

# Round-robin seguro
cycle = itertools.cycle(worker_uris)
lock = threading.Lock()

# 🔥 Cache de proxies (mejora rendimiento)
proxies = {uri: Pyro5.api.Proxy(uri) for uri in worker_uris}

@Pyro5.api.expose
class LoadBalancer:

    def comprar_entrada(self, client_id, request_id):
        print(f"{client_id} -> {request_id}")
        with lock:
            uri = next(cycle)
        try:
            with Pyro5.api.Proxy(uri) as worker:
                return worker.comprar_entrada(client_id, request_id)
        except Exception:
            return False


if __name__ == "__main__":
    daemon = Pyro5.api.Daemon(port=9000)
    ns = Pyro5.api.locate_ns()

    lb = LoadBalancer()
    uri = daemon.register(lb)

    ns.register("ticket.server.unnumbered", uri)

    print("✅ LoadBalancer corriendo:", uri)
    daemon.requestLoop()