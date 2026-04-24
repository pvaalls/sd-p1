import Pyro5.api
import Pyro5.errors
import time
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

def procesar_request(uri, request):
    parts = request.strip().split()
    if len(parts) != 3 or parts[0] != 'BUY':
        return False, 0.0

    client_id = parts[1]
    request_id = parts[2]

    start = time.time()

    try:
        with Pyro5.api.Proxy(uri) as ticket_server:
            ticket_server._pyroBind()
            result = ticket_server.comprar_entrada(client_id, request_id)
    except Exception:
        return False, 0.0

    latency = time.time() - start
    return result, latency

def comprar_entradas(uri, file, num_threads):
    with open(file, 'r') as f:
        requests = f.readlines()

    total_requests = len(requests)
    entradas_compradas = 0
    latencias = []

    start_time = time.time()

    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(procesar_request, uri, r) for r in requests]

        for future in as_completed(futures):
            result, latency = future.result()
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
    parser.add_argument("-n", "--ns", type=str, default="localhost", help="NameServer host (default: %(default)s)")
    request_file = Path(__file__).resolve().parent / "../data/benchmark_unnumbered_20000.txt"
    parser.add_argument("-f", "--file", type=str, default=request_file, help="Request File")
    parser.add_argument("-t", "--threads", type=int, default=10, help="Number of Threads")
    args = parser.parse_args()

    try:
        server = "ticket.server.unnumbered"

        print(f"[+] NameServer: {args.ns}:9090")
        ns = Pyro5.api.locate_ns(host=args.ns, port=9090)

        print(f"[+] Resolviendo: {server}")
        uri = ns.lookup(server)
        print(f"[+] URI: {uri}")
        print(f"[+] Threads: {args.threads}")

        with Pyro5.api.Proxy(uri) as loadbalancer:
            worker_uri = loadbalancer.get_worker()
            if worker_uri is None:
                exit("no worker")

        print(f"[+] Worker URI: {worker_uri}")
        print()

        input("[+] Pulsa ENTER para empezar...")

        stats = comprar_entradas(worker_uri, args.file, args.threads)

        # --- AÑADIDO: OBTENER STATS DEL SERVER ---
        try:
            with Pyro5.api.Proxy(worker_uri) as worker:
                srv_reqs, srv_avg_time = worker.get_stats()
        except:
            srv_avg_time = 0

        print("\n=== RESULTADOS: Cliente ===")
        print(f"Total requests       : {stats['total_requests']}")
        print(f"Entradas compradas   : {stats['entradas']}")
        print(f"Tiempo total (s)     : {stats['total_time']:.4f}")
        print(f"Throughput (req/s)   : {stats['throughput']:.2f}")
        print(f"Latencia media (CLT) : {stats['avg_latency']:.6f}")
        print("\n=== RESULTADOS: Servidor ===")
        print(f"Total requests       : {srv_reqs}")
        print(f"Total Service time   : {srv_avg_time:.6f}")

    except Pyro5.errors.NamingError:
        print("Error: No se encuentra el servidor.")
    except Exception as e:
        print(f"Error inesperado: {e}")

if __name__ == "__main__":
    main()
