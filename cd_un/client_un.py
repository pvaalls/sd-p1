import Pyro5.api
import Pyro5.errors
import time
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

def procesar_request(proxy, request):
    parts = request.strip().split()
    if len(parts) != 3 or parts[0] != 'BUY':
        return False, 0.0

    client_id  = parts[1]
    request_id = parts[2]

    start = time.time()

    try:
        # El proxy realiza la llamada RMI
        result = proxy.comprar_entrada(client_id, request_id)
    except Exception:
        # Si falla aquí, lanzamos la excepción para que el hilo la gestione
        raise

    latency = time.time() - start
    return result, latency

# --- MODIFICADO: Ahora gestiona la reconexión si falla el worker ---
def worker_thread_logic(server_uri, worker_uri, requests_chunk):
    results = []
    current_worker_uri = worker_uri

    # Usamos un índice para saber por qué petición íbamos si falla
    i = 0
    while i < len(requests_chunk):
        try:
            with Pyro5.api.Proxy(current_worker_uri) as proxy:
                proxy._pyroBind()
                while i < len(requests_chunk):
                    res, lat = procesar_request(proxy, requests_chunk[i])
                    results.append((res, lat))
                    i += 1
        except Exception as e:
            print(f"[!] Worker fallido ({current_worker_uri}). Solicitando uno nuevo...")
            try:
                # Pedimos al servidor principal un nuevo worker
                with Pyro5.api.Proxy(server_uri) as server:
                    current_worker_uri = server.get_worker()
                if current_worker_uri is None:
                    print("[-] No hay workers disponibles.")
                    break
                print(f"[+] Nuevo Worker obtenido: {current_worker_uri}")
            except Exception:
                print("[-] Error crítico: No se puede contactar con el Servidor Principal.")
                break
    return results

def comprar_entradas(server_uri, worker_uri, file, num_threads):
    with open(file, 'r') as f:
        requests = f.readlines()

    total_requests = len(requests)
    entradas_compradas = 0
    latencias = []

    chunk_size = (total_requests // num_threads) + 1
    chunks = [requests[i:i + chunk_size] for i in range(0, total_requests, chunk_size)]

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # --- MODIFICADO: Pasamos el server_uri para permitir recuperación ---
        futures = [executor.submit(worker_thread_logic, server_uri, worker_uri, chunk) for chunk in chunks]

        for future in as_completed(futures):
            batch_results = future.result()
            for result, latency in batch_results:
                if result:
                    entradas_compradas += 1
                if latency > 0:
                    latencias.append(latency)

    end_time = time.time()

    total_time = end_time - start_time
    throughput = total_requests / total_time if total_time > 0 else 0
    avg_latency = sum(latencias) / len(latencias) if latencias else 0

    return {
        "total_requests": total_requests,
        "entradas": entradas_compradas,
        "total_time": total_time,
        "throughput": throughput,
        "avg_latency": avg_latency
    }

def main():
    parser = argparse.ArgumentParser(description="Benchmark concurrente Pyro5")
    parser.add_argument("-n", "--ns", type=str, default="localhost", help="NameServer host")
    request_file = Path(__file__).resolve().parent / "../data/benchmark_unnumbered_20000.txt"
    parser.add_argument("-f", "--file", type=str, default=str(request_file), help="Request File")
    parser.add_argument("-t", "--threads", type=int, default=10, help="Number of Threads")
    args = parser.parse_args()

    try:
        server_name = "ticket.server.unnumbered"

        print(f"[+] NameServer  : {args.ns}:9090")
        ns = Pyro5.api.locate_ns(host=args.ns, port=9090)
        server_uri = ns.lookup(server_name)

        print(f"[+] Server URI  : {server_uri}")
        print(f"[+] Threads     : {args.threads}")

        with Pyro5.api.Proxy(server_uri) as server:
            worker_uri = server.get_worker()
            if worker_uri is None:
                exit("no worker")

        print(f"[+] Worker URI  : {worker_uri}")
        input("[+] Pulsa ENTER para empezar...")

        # Pasamos ambos URIs para la lógica de reintento
        stats = comprar_entradas(server_uri, worker_uri, args.file, args.threads)

        print("\n=== RESULTADOS: Cliente ===")
        print(f"Total requests       : {stats['total_requests']}")
        print(f"Entradas compradas   : {stats['entradas']}")
        print(f"Tiempo total (s)     : {stats['total_time']:.4f}")
        print(f"Throughput (req/s)   : {stats['throughput']:.2f}")
        print(f"Latencia media (CLT) : {stats['avg_latency']:.6f}")

    except Pyro5.errors.NamingError:
        print("Error: No se encuentra el servidor NameServer.")
    except Exception as e:
        print(f"Error inesperado: {e}")

if __name__ == "__main__":
    main()
