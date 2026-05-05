import Pyro5.api
import sys
import signal
import psycopg2
import argparse
import time
import threading

DB_NAME = "concerts"
DB_USER = "postgres"
DB_PASS = "admin"

@Pyro5.api.expose
class Worker:

    def __init__(self, verbose, postgres_host):
        self.vprint = print if verbose else lambda *args, **kwargs: None

        # Conexión a Postgres
        self.conn = psycopg2.connect(
            host     = postgres_host,
            database = DB_NAME,
            user     = DB_USER,
            password = DB_PASS
        )


        self.stats_lock = threading.Lock()
        self.total_requests = 0
        # Guardaremos una lista de tuplas de las peticiones (inicio, fin)
        self.intervalos = []

    def comprar_entrada(self, client_id, seat_id, request_id):
        self.vprint(f"{client_id} -> {request_id}")

        inicio = time.perf_counter()

        try:
            cursor = self.conn.cursor()

            # S'intenta inserir el seient. Si ja existeix, es descarta l'operació i no retorna res.
            cursor.execute("""
                INSERT INTO cd_num (seat_id, client_id)
                VALUES (%s, %s)
                ON CONFLICT (seat_id) DO NOTHING
                RETURNING seat_id;
            """, (seat_id, client_id))

            result = cursor.fetchone()
            self.conn.commit()
            cursor.close()
        except Exception as e:
            print("Error en Worker:", e)
            self.conn.rollback()
            result = False

        fin = time.perf_counter()

        # Guardar Intervalo de esta Petición
        with self.stats_lock:
            self.total_requests += 1
            self.intervalos.append((inicio, fin))

        return result

    # Algoritmo de unión de intervalos
    def _tiempo_real_sin_solapamiento(self):
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
            real_time = self._tiempo_real_sin_solapamiento()
            return {
                "total_requests": self.total_requests,
                "total_service_time": self._tiempo_real_sin_solapamiento(),
            }

    def reset_stats(self):
        with self.stats_lock:
            self.total_requests = 0
            self.intervalos = []

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

    parser = argparse.ArgumentParser(description="Worker (Numbered Tickets)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Specifies to use the verbose mode")
    parser.add_argument("-n", "--ns", type=str, default="localhost", help="Nameserver (default: %(default)s)")
    parser.add_argument("-H", "--host", type=str, default="localhost", help="Host (default: %(default)s)")
    parser.add_argument("-P", "--postgres", type=str, default="localhost", help="Postgres Host (default: %(default)s)")
    parser.add_argument("-p", "--port", type=int, required=True, help="Port")
    args = parser.parse_args()

    daemon = Pyro5.api.Daemon(host=args.host,port=args.port)

    uri    = daemon.register(Worker(args.verbose,args.postgres), objectId="worker")

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
