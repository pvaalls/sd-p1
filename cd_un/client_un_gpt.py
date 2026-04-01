import Pyro5.api
import Pyro5.errors
import time
from concurrent.futures import ThreadPoolExecutor

MAX_WORKERS = 50

def send_request(line):
    parts = line.strip().split()
    if len(parts) == 3 and parts[0] == 'BUY':
        client_id = parts[1]
        request_id = parts[2]

        # Proxy por hilo
        with Pyro5.api.Proxy("PYRONAME:ticket.server.unnumbered") as ticket_server:
            v = ticket_server.comprar_entrada(client_id, request_id)
            print(v)

def main():
    try:
        with open("../data/benchmark_unnumbered_20000.txt", 'r') as f:
            lines = f.readlines()
            
        start_time = time.time()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            list(executor.map(send_request, lines))

        end_time = time.time()
        print(f"✅ Benchmark completado en {end_time - start_time:.4f} segundos")

    except Pyro5.errors.NamingError:
        print("❌ No se encuentra el servidor (Name Server o LB no activos)")
    except Exception as e:
        print(f"❌ Error inesperado: {e}")

if __name__ == "__main__":
    main()