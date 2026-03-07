# import Pyro5.api

# worker_uris = [
#     "PYRO:worker@localhost:9101",
#     "PYRO:worker@localhost:9102",
# ]

# @Pyro5.api.expose
# class LoadBalancer:
#     it = 0

#     def comprar_entrada(self, client_id, request_id):
#         uri = worker_uris[ self.it % len(worker_uris) ]
#         self.it += 1
#         with Pyro5.api.Proxy(uri) as worker:
#             return worker.comprar_entrada(client_id, request_id)

# def main ():

#     ## Iniciar Pyro ##
#     daemon = Pyro5.api.Daemon(port=9000)            # Escuchar Peticiones
#     ns     = Pyro5.api.locate_ns()                  # Buscar el Name Server
#     uri    = daemon.register(LoadBalancer())        # Registrem la classe
#     ns.register("ticket.server.unnumbered", uri)    # Nombrar el Name Server

#     print("Unnumbered Ticket Server (LB) ready: ", uri)
#     daemon.requestLoop()                            # Escuchar Peticiones

# if __name__ == "__main__":
#     main()

import Pyro5.api
import itertools
import threading

# URIs de los workers (objectId = "worker")
worker_uris = [
    "PYRO:worker@localhost:9101",
    "PYRO:worker@localhost:9102",
]

# Round-robin seguro
cycle = itertools.cycle(worker_uris)
lock = threading.Lock()

@Pyro5.api.expose
class LoadBalancer:

    def comprar_entrada(self, client_id, request_id):
        # Selección segura entre threads
        with lock:
            uri = next(cycle)

        # Crear proxy por cada llamada (thread-safe)
        with Pyro5.api.Proxy(uri) as worker:
            return worker.comprar_entrada(client_id, request_id)

if __name__ == "__main__":
    daemon = Pyro5.api.Daemon(port=9000)
    ns = Pyro5.api.locate_ns()

    lb = LoadBalancer()
    uri = daemon.register(lb)
    ns.register("ticket.server.unnumbered", uri)

    print("LoadBalancer corriendo, URI registrada en NameServer:", uri)
    daemon.requestLoop()