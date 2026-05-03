import pika
import json
import time

# config
RABBIT_HOST = 'localhost'
QUEUE_NAME = 'cues_compra'
FITXER_BENCHMARK = "../data/benchmark_unnumbered_20000.txt"

class TicketClientFireAndForget:
    def __init__(self):
        # Connec a rabbit
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
        self.channel = self.connection.channel()

        # Activar Publisher Confirms per garantir que RabbitMQ escriu al disc
       # self.channel.confirm_delivery()

        # Declarar la cua com a persistent (sobreviurà a reinicis del servidor)
        self.channel.queue_declare(queue=QUEUE_NAME, durable=True)

        self.total_peticions = 0

    def enviar_peticio(self, client_id, request_id):
        # Envia una única petició a la cua principal sense esperar resposta
        missatge = {
            'client_id': client_id,
            'request_id': request_id
        }

        try:
            self.channel.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=json.dumps(missatge),
                properties=pika.BasicProperties(
                    delivery_mode=2, # Marca el missatge físic com a persistent
                ),
                mandatory=True # Força a llançar excepció si la cua no existeix
            )
            self.total_peticions += 1
        except pika.exceptions.UnroutableError:
            print(f"Error: El missatge de la petició {request_id} no s'ha pogut confirmar al disc.")

def main():
    client = TicketClientFireAndForget()

    try:
        # es llegeix el fitxer
        with open(FITXER_BENCHMARK, 'r') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Error: No es troba el fitxer {FITXER_BENCHMARK}")
        return

    print("Enviant peticions a RabbitMQ (Mode Fire-and-Forget)...")
    start_time = time.time()

    # enviem les peticions
    for line in lines:
        parts = line.strip().split()
        if len(parts) == 3 and parts[0] == 'BUY':
            client_id = parts[1]
            request_id = parts[2]
            client.enviar_peticio(client_id, request_id)

    end_time = time.time()
    temps_total = end_time - start_time
    
    # Aquest throughput només mesura la velocitat d'injecció a la cua, no el processament dels workers
    throughput = client.total_peticions / temps_total if temps_total > 0 else 0

    print(f"Injecció de càrrega finalitzada en {temps_total:.4f} segons.")
    print(f"Peticions publicades a la cua persistent: {client.total_peticions}")
    print(f"Velocitat d'injecció: {throughput:.2f} missatges/seg")
    print("El client finalitza aquí. El monitor s'encarregarà de calcular les mètriques de processament.")

    client.connection.close()

if __name__ == "__main__":
    main()