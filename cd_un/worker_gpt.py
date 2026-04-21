import Pyro5.api
import sys
import signal
import redis
import argparse
import threading

@Pyro5.api.expose
class Worker:
    limite_entradas = 20000
    BATCH_SIZE = 50  # Tamaño del lote que se pide a Redis

    def __init__(self, verbose, redis_host):
        self.redis_host = redis_host
        self.vprint = print if verbose else lambda *args, **kwargs: None

        # Estado local del cupo (Thread-safe)
        self.lote_local = 0
        self.lock_local = threading.Lock()

        # Conexión a Redis
        try:
            self.redis_server = redis.Redis(
                host=self.redis_host, 
                port=6379, 
                db=0, 
                decode_responses=True
            )
            # Inicializa el contador solo si no existe
            self.redis_server.setnx("entradas_vendidas_un", 0)
        except Exception as e:
            print(f"[\033[31m!\033[0m] Error conectando a Redis: {e}")
            sys.exit(1)

    def _obtener_mas_tickets(self):
        """Solicita un lote a Redis de forma atómica usando INCRBY"""
        try:
            # Incrementamos el contador global por el tamaño del lote
            nuevo_total_global = self.redis_server.incrby("entradas_vendidas_un", self.BATCH_SIZE)
            
            # Cálculo de cuántas entradas nos corresponden realmente
            if nuevo_total_global <= self.limite_entradas:
                # Caso normal: hay suficientes para el lote completo
                self.lote_local = self.BATCH_SIZE
                self.vprint(f"[\033[34mBatch\033[0m] Adquirido lote de {self.BATCH_SIZE}. Total global: {nuevo_total_global}")
            elif (nuevo_total_global - self.BATCH_SIZE) < self.limite_entradas:
                # Caso borde: El lote pedido supera el límite, cogemos solo las que sobran
                restantes = self.limite_entradas - (nuevo_total_global - self.BATCH_SIZE)
                self.lote_local = restantes
                self.vprint(f"[\033[33mBatch\033[0m] Lote parcial: {restantes} entradas restantes.")
            else:
                # El inventario ya estaba agotado antes de pedir
                self.lote_local = 0
                self.vprint("[\033[31mBatch\033[0m] Agotado: No quedan entradas en Redis.")
        except Exception as e:
            print(f"Error en operación Redis: {e}")
            self.lote_local = 0

    def comprar_entrada(self, client_id, request_id):
        """Gestiona la compra desde el cupo local"""
        with self.lock_local:
            # Si no hay tickets locales, intentamos recargar
            if self.lote_local <= 0:
                self._obtener_mas_tickets()

            # Verificamos si la recarga fue exitosa
            if self.lote_local > 0:
                self.lote_local -= 1
                self.vprint(f"[\033[32mOK\033[0m] {client_id} -> {request_id} (Restantes en worker: {self.lote_local})")
                return True
            else:
                return False

def register_to_lb(uri, ns_host, lb_ns_entry="ticket.server.unnumbered"):
    try:
        ns = Pyro5.api.locate_ns(host=ns_host)
        lb_uri = ns.lookup(lb_ns_entry)
        lb = Pyro5.api.Proxy(lb_uri)
        lb.register_worker(uri)
        print("[\033[32m+\033[0m] Status: Registered on Load Balancer")
        return lb
    except Exception as e:
        print(f"[\033[31m!\033[0m] Status: Not registered on LB ({e})")
        return None

def main():
    parser = argparse.ArgumentParser(description="Worker con Batching")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-n", "--ns", type=str, default="localhost", help="Nameserver host")
    parser.add_argument("-H", "--host", type=str, default="localhost", help="Worker host")
    parser.add_argument("-R", "--redis", type=str, default="localhost", help="Redis host")
    parser.add_argument("-p", "--port", type=int, required=True, help="Worker port")
    args = parser.parse_args()

    daemon = Pyro5.api.Daemon(host=args.host, port=args.port)
    worker_instance = Worker(args.verbose, args.redis)
    uri = daemon.register(worker_instance, objectId="worker")

    print(f"[\033[32m+\033[0m] Worker activo en {uri}")
    
    lb = register_to_lb(uri, args.ns)
    if lb is None:
        sys.exit(1)

    def shutdown(signum, frame):
        print("\n[\033[33m!\033[0m] Cerrando worker...")
        try:
            lb.unregister_worker(uri)
        except:
            pass
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    daemon.requestLoop()

if __name__ == "__main__":
    main()