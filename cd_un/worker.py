# import Pyro5.api
# import sys
# import redis 

# @Pyro5.api.expose
# class Worker:
    
#     limite_entradas = 20000

#     def __init__ ( self ):
#         # Conexión a Redis
#         self.redisserver = redis.Redis(host='localhost', port=6379, db=0)

#         # Inicializar el contador a 0
#         self.redisserver.setnx('entrades_venudes', 0)
        
#     """
#     Intenta comprar una entrada.
#     Retorna True si la compra és exitosa, False si s'han esgotat.
#     """
#     def comprar_entrada ( self, client_id, request_id ):
#         # Incrementa y devuelve el numero de entradas actuales vendidas
#         entradas = self.redisserver.incr('entrades_venudes')

#         if entradas <= self.limite_entradas:
#             print(f"Client {client_id} (Req: {request_id}) -> COMPRA OK ({entradas}/{self.limite_entradas})")
#             return True
#         else:
#             print(f"Client {client_id} (Req: {request_id}) -> ESGOTADES")
#             return False

# daemon = Pyro5.api.Daemon(port=int(sys.argv[1]))
# uri = daemon.register(Worker(), objectId="worker")

# print("Worker running:", uri)

# daemon.requestLoop()

import Pyro5.api
import sys
import redis

@Pyro5.api.expose
class Worker:

    limite_entradas = 20000

    def __init__(self):
        # Conexión a Redis
        self.redisserver = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
        # Solo inicializa si no existe
        self.redisserver.setnx("entrades_venudes", 0)

    def comprar_entrada(self, client_id, request_id):
        try:
            entradas = self.redisserver.incr("entrades_venudes")
            if entradas <= self.limite_entradas:
                # Comenta print si haces 20k requests para no saturar consola
                # print(f"Client {client_id} (Req: {request_id}) -> COMPRA OK ({entradas}/{self.limite_entradas})")
                return True
            else:
                return False
        except Exception as e:
            print("Error en Worker:", e)
            return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Uso: python worker.py <PUERTO>")
        sys.exit(1)

    port = int(sys.argv[1])
    daemon = Pyro5.api.Daemon(port=port)

    # ObjectId fijo para que el LB se conecte
    worker_obj = Worker()
    uri = daemon.register(worker_obj, objectId="worker")
    print(f"Worker corriendo en puerto {port}, URI: {uri}")

    daemon.requestLoop()