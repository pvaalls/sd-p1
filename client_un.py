import Pyro5.api
import Pyro5.errors
import time

def main ():
    # Busquem el servei pel nom que hem registrat abans
    ticket_server = Pyro5.api.Proxy("PYRONAME:ticket.server.unnumbered")
    
    try:
        with open("benchmark_unnumbered_20000.txt", 'r') as f:
            lines = f.readlines()

        start_time = time.time()
        
        for line in lines:
            # Format del fitxer: BUY <client_id> <request_id>
            parts = line.strip().split()
            if len(parts) == 3 and parts[0] == 'BUY':
                client_id  = parts[1]
                request_id = parts[2]
                
                # Cridem al mètode remot (com si fos local)
                resultat = ticket_server.comprar_entrada(client_id, request_id)
                
        end_time = time.time()
        print(f"Benchmark finalitzat en {end_time - start_time:.4f} segons.")

    except Pyro5.errors.NamingError:
        print("Error: No es troba el servidor. Assegura't que el Name Server i el Servidor corren.")
    except Exception as e:
        print(f"Error inesperat: {e}")

if __name__ == "__main__":
    # Assegura't de tenir el fitxer txt al mateix directori o canvia el nom
    main()