import Pyro5.api
import sys
import signal
import redis
import argparse
import time
import threading

@Pyro5.api.expose
class Worker:

    limite_entradas = 20000
    redis_key = "entradas_vendidas_un"

    def __init__(self, verbose, redis_host):
        self.vprint = print if verbose else lambda *args, **kwargs: None
        self.redis_server = redis.Redis(host=redis_host, port=6379, db=0, decode_responses=True)
        self.redis_server.setnx(self.redis_key, 0)

        # --- ESTADÍSTICAS MEJORADAS ---
        self.stats_lock = threading.Lock()
        self.total_requests = 0
        # Guardaremos una lista de tuplas (inicio, fin)
        self.intervalos = []

    def comprar_entrada(self, client_id, request_id):
        self.vprint(f"{client_id} -> {request_id}")
        start_time = time.perf_counter()

        try:
            entradas = self.redis_server.incr(self.redis_key)
            result = entradas <= self.limite_entradas
        except Exception as e:
            print("Error en Worker:", e)
            result = False

        end_time = time.perf_counter()

        # Guardamos el intervalo específico de esta petición
        with self.stats_lock:
            self.total_requests += 1
            self.intervalos.append((start_time, end_time))

        return result

    def _calcular_tiempo_real_sin_solapamiento(self):
        """Algoritmo de unión de intervalos"""
        if not self.intervalos:
            return 0.0

        # 1. Ordenar intervalos por tiempo de inicio
        # Usamos una copia para no bloquear el procesamiento mientras calculamos
        sorted_intervals = sorted(self.intervalos, key=lambda x: x[0])

        if not sorted_intervals:
            return 0.0

        total_time = 0.0
        curr_start, curr_end = sorted_intervals[0]

        for next_start, next_end in sorted_intervals[1:]:
            if next_start <= curr_end:
                # Hay solapamiento, extendemos el final si es necesario
                curr_end = max(curr_end, next_end)
            else:
                # No hay solapamiento, sumamos el tramo anterior y empezamos uno nuevo
                total_time += curr_end - curr_start
                curr_start, curr_end = next_start, next_end

        # Sumar el último tramo
        total_time += curr_end - curr_start
        return total_time

    def get_stats(self):
        with self.stats_lock:
            real_time = self._calcular_tiempo_real_sin_solapamiento()
            return {
                "total_requests": self.total_requests,
                "total_service_time": real_time,
                "total_intervals_recorded": len(self.intervalos)
            }

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
        sys.exit(1)
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

    def shutdown(signum, frame):
        print("[\033[33m!\033[0m] - Aborting...")

        try:
            lb.unregister_worker(uri)
        except Exception as e:
            print("Error al desregistrar:", e)
        sys.exit(1)

    signal.signal(signal.SIGINT, shutdown)

    daemon.requestLoop()

if __name__ == "__main__":
    main()
