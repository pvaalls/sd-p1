import Pyro5.api
import redis

@Pyro5.api.expose
class TicketServer ():
    
    limite_entradas = 20000

    def __init__ ( self ):
        # Conexión a Redis
        self.redisserver = redis.Redis(host='localhost', port=6379, db=0)

        # Inicializar el contador a 0
        self.redisserver.set('entrades_venudes', 0)
        
    """
    Intenta comprar una entrada.
    Retorna True si la compra és exitosa, False si s'han esgotat.
    """
    def comprar_entrada ( self, client_id, request_id ):
        # Incrementa y devuelve el numero de entradas actuales vendidas
        entradas = self.redisserver.incr('entrades_venudes')

        if entradas <= self.limite_entradas:
            print(f"Client {client_id} (Req: {request_id}) -> COMPRA OK ({entradas}/{self.limite_entradas})")
            return True
        else:
            print(f"Client {client_id} (Req: {request_id}) -> ESGOTADES")
            return False

def main ():

    ## Iniciar Pyro ##
    daemon = Pyro5.api.Daemon()                     # Escuchar Peticiones
    ns     = Pyro5.api.locate_ns()                  # Buscar el Name Server
    uri    = daemon.register(TicketServer())        # Registrem la classe
    ns.register("ticket.server.unnumbered", uri)    # Nombrar el Name Server

    print("Unnumbered Ticket Server ready...")
    daemon.requestLoop()                            # Escuchar Peticiones

if __name__ == "__main__":
    main()