import Pyro5.api
import itertools
import threading
import sys
import argparse

# Configuración de alto rendimiento para Pyro5
Pyro5.config.SERVERTYPE = "thread"
Pyro5.config.THREADPOOL_SIZE = 100  # Capacidad para procesar peticiones en paralelo

counter = itertools.count()
thread_local = threading.local()

def get_proxy(uri):
    """Mantiene un proxy por cada hilo para evitar overhead de conexión"""
    if not hasattr(thread_local, "proxies"):
        thread_local.proxies = {}
    if uri not in thread_local.proxies:
        proxy = Pyro5.api.Proxy(uri)
        proxy._pyroBind()
        thread_local.proxies[uri] = proxy
    return thread_local.proxies[uri]

@Pyro5.api.expose
class LoadBalancer:
    def __init__(self, verbose):
        self.vprint = print if verbose else lambda *args, **kwargs: None
        self.worker_uris = []
        self.lock = threading.Lock()

    def register_worker(self, uri):
        with self.lock:
            if uri not in self.worker_uris:
                self.worker_uris.append(uri)
                print(f"[\033[32m+\033[0m] Nuevo Worker registrado: {uri}")

    def unregister_worker(self, uri):
        with self.lock:
            if uri in self.worker_uris:
                self.worker_uris.remove(uri)
                print(f"[\033[31m-\033[0m] Worker eliminado: {uri}")

    def get_next_worker(self):
        with self.lock:
            if not self.worker_uris:
                return None
            # Round Robin
            idx = next(counter) % len(self.worker_uris)
            return self.worker_uris[idx]

    def comprar_entrada(self, client_id, request_id):
        uri = self.get_next_worker()
        if uri is None:
            print("[\033[31m!\033[0m] Error: No hay workers disponibles.")
            return False

        try:
            worker = get_proxy(uri)
            # La llamada es síncrona pero muy rápida gracias al batching del worker
            return worker.comprar_entrada(client_id, request_id)
        except Exception as e:
            print(f"[\033[31m!\033[0m] Worker fallido detectado: {uri}")
            self.unregister_worker(uri)
            return False

def main():
    parser = argparse.ArgumentParser(description="Load Balancer Unnumbered")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-n", "--ns", type=str, default="localhost", help="Nameserver host")
    parser.add_argument("-H", "--host", type=str, default="localhost", help="LB host")
    parser.add_argument("-p", "--port", type=int, default=9000, help="LB port")
    args = parser.parse_args()

    lb_name = "ticket.server.unnumbered"
    daemon = Pyro5.api.Daemon(host=args.host, port=args.port)
    
    try:
        ns = Pyro5.api.locate_ns(host=args.ns)
        uri = daemon.register(LoadBalancer(args.verbose), objectId="loadbalancer")
        ns.register(lb_name, uri)
    except Exception as e:
        print(f"[\033[31m!\033[0m] Error iniciando LB: {e}")
        sys.exit(1)

    print(f"[\033[32m+\033[0m] LoadBalancer ejecutándose en {uri}")
    print(f"[\033[32m+\033[0m] Registrado en NameServer como: {lb_name}")

    daemon.requestLoop()

if __name__ == "__main__":
    main()