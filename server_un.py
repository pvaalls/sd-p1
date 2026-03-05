import Pyro4
import redis

# Configuració de Redis (base de dades en memòria)
r = redis.Redis(host='localhost', port=6379, db=0)

# Inicialitzem el comptador a 0 (només per proves, en producció compte amb reiniciar-lo!)
r.set('entrades_venudes', 0)
LIMIT_ENTRADES = 20000

@Pyro4.expose
class TicketServer(object):
    def comprar_entrada(self, client_id, request_id):
        """
        Intenta comprar una entrada.
        Retorna True si la compra és exitosa, False si s'han esgotat.
        """
        # INCR és atòmic: incrementa i retorna el nou valor en un sol pas.
        # Això evita que dos processos llegeixin el mateix valor alhora.
        noves_vendes = r.incr('entrades_venudes')

        if noves_vendes <= LIMIT_ENTRADES:
            print(f"Client {client_id} (Req: {request_id}) -> COMPRA OK ({noves_vendes}/20000)")
            return True
        else:
            # Opcional: Si ens passem, podem decrementar per mantenir el comptador "net",
            # però per rendiment sovint es deixa pujar.
            print(f"Client {client_id} (Req: {request_id}) -> ESGOTADES")
            return False

def main():
    # Inicialitzem el dimoni de Pyro (el que escolta peticions)
    daemon = Pyro4.Daemon() 
    
    # Busquem el Name Server (el directori de serveis)
    ns = Pyro4.locateNS() 
    
    # Registrem la classe
    uri = daemon.register(TicketServer)
    
    # Posem el nom al Name Server perquè el client el trobi
    ns.register("ticket.server.unnumbered", uri)

    print("Servidor Pyro (No Numerat) llest.")
    # Entra en bucle esperant peticions
    daemon.requestLoop()

if __name__ == "__main__":
    main()