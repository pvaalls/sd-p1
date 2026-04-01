import Pyro5.api
import Pyro5.errors
import time
import argparse

def comprar_entradas ( uri ):
    entradas_compradas = 0
    # Busquem el servei pel nom que hem registrat abans
    with Pyro5.api.Proxy(uri) as ticket_server:
        with open("../data/benchmark_unnumbered_20000.txt", 'r') as f:
            for request in f:
                # Format del fitxer: BUY <client_id> <request_id>
                parts = request.strip().split()
                if len(parts) == 3 and parts[0] == 'BUY':
                    client_id  = parts[1]
                    request_id = parts[2]
                    
                    # Cridem al mètode remot (com si fos local)
                    resultat = ticket_server.comprar_entrada(client_id, request_id)
                    if resultat:
                        entradas_compradas += 1

    return entradas_compradas

def main ():

    parser = argparse.ArgumentParser(description="Load Balancer")
    parser.add_argument("-n", "--ns", type=str, default="localhost", help="Specifies to use the given nameserver (default: %(default)s)")
    args = parser.parse_args()

    try:
        server = "ticket.server.unnumbered"

        print(f"[\033[32m+\033[0m] - Looking for Name Server at :", f"\033[32m{args.ns}:9090\033[0m")
        ns = Pyro5.api.locate_ns(host=args.ns, port=9090)

        print(f"[\033[32m+\033[0m] - Resolving server :", f"\033[32m{server}\033[0m")
        uri = ns.lookup(server)

        print(f"[\033[32m+\033[0m] - Got :", f"\033[32m{uri}\033[0m")
        
        start_time = time.time()

        input(f"[\033[32m+\033[0m] - Press any key to start...")
        entradas = comprar_entradas( uri )

        end_time = time.time()
        
        print(f" Benchmark finalizado en : {end_time - start_time:.4f} segons.")
        print(f"      Entradas compradas : {entradas}")

    except Pyro5.errors.NamingError:
        print("Error: No es troba el servidor. Assegura't que el Name Server i el Servidor corren.")
    except Exception as e:
        print(f"Error inesperat: {e}")

if __name__ == "__main__":
    main()