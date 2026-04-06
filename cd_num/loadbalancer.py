import Pyro5.api
import itertools
import threading
import sys
import argparse

Pyro5.config.SERVERTYPE = "thread"
Pyro5.config.THREADPOOL_SIZE = 50

counter = itertools.count()
thread_local = threading.local()

def get_proxy(uri):
    if not hasattr(thread_local, "proxies"):
        thread_local.proxies = {}

    if uri not in thread_local.proxies:
        proxy = Pyro5.api.Proxy(uri)
        proxy._pyroBind()
        thread_local.proxies[uri] = proxy

    return thread_local.proxies[uri]

@Pyro5.api.expose
class LoadBalancer:

    def __init__(self):
        self.worker_uris = []
        self.lock = threading.Lock()

    def register_worker(self, uri):
        with self.lock:
            if uri not in self.worker_uris:
                self.worker_uris.append(uri)
                print(f"[\033[32m+\033[0m] - Worker + :", f"\033[32m{uri}\033[0m")

    def unregister_worker(self, uri):
        with self.lock:
            if uri in self.worker_uris:
                self.worker_uris.remove(uri)
                print(f"[\033[31m-\033[0m] - Worker - :", f"\033[31m{uri}\033[0m")

    def get_next_worker(self):
        with self.lock:
            if not self.worker_uris:
                return None
            return self.worker_uris[next(counter) % len(self.worker_uris)]

    def comprar_entrada(self, client_id, seat_id, request_id):
        print(f"{client_id} -> {seat_id} -> {request_id}")

        uri = self.get_next_worker()
        if uri is None:
            print("No hay workers disponibles")
            return False

        try:
            worker = get_proxy(uri)
            return worker.comprar_entrada(client_id, seat_id, request_id)
        except Exception as e:
            print("Worker caído:", e)
            self.unregister_worker(uri)  # 🔥 auto-limpieza
            return False

def main():

    parser = argparse.ArgumentParser(description="Load Balancer (Numbered Tickets)")
    parser.add_argument("-n", "--ns", type=str, default="localhost", help="Specifies to use the given nameserver (default: %(default)s)")
    parser.add_argument("-H", "--host", type=str, default="localhost", help="Specifies to use the given host (default: %(default)s)")
    parser.add_argument("-p", "--port", type=int, default=9000, help="Specifies to use the given port (default: %(default)s)")
    args = parser.parse_args()

    lb_name = "ticket.server.numbered"
    
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