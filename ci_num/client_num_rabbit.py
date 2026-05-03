import pika
import json
import time

# config
RABBIT_HOST = 'localhost'
QUEUE_NAME = 'cues_compra_num'
#FITXER_BENCHMARK = "../data/benchmark_numbered_60000.txt"
FITXER_BENCHMARK = "../data/benchmark_hotspot_60000.txt"

class TicketClientFireAndForgetNum:
    def __init__(self):
        # Connexió a RabbitMQ
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
        self.channel = self.connection.channel()

        # Declarar la cua principal com a persistent
        self.channel.queue_declare(queue=QUEUE_NAME, durable=True)

        self.total_peticions = 0

    def enviar_peticio(self, client_id, seat_id, request_id):
        # Envia una única petició a la cua principal sense esperar resposta
        missatge = {
            'client_id': client_id,
            'seat_id': seat_id,
            'request_id': request_id
        }

        self.channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            body=json.dumps(missatge),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Marca el missatge físic com a persistent al disc
            )
        )
        self.total_peticions += 1

def main():
    client = TicketClientFireAndForgetNum()

    try:
        # Llegir el fitxer de dades
        with open(FITXER_BENCHMARK, 'r') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Error: No es troba el fitxer {FITXER_BENCHMARK}")
        return

    print("Enviant peticions numerades a RabbitMQ (Mode Fire-and-Forget)...")
    start_time = time.time()

    # Enviament de peticions
    for line in lines:
        parts = line.strip().split()
        if len(parts) == 4 and parts[0] == 'BUY':
            client_id = parts[1]
            seat_id = parts[2]
            request_id = parts[3]
            
            client.enviar_peticio(client_id, seat_id, request_id)

    end_time = time.time()
    temps_total = end_time - start_time
    
    # Aquest throughput mesura exclusivament la velocitat d'injecció a la cua
    throughput = client.total_peticions / temps_total if temps_total > 0 else 0

    print(f"Injecció de càrrega finalitzada en {temps_total:.4f} segons.")
    print(f"Peticions publicades a la cua persistent: {client.total_peticions}")
    print(f"Velocitat d'injecció: {throughput:.2f} missatges/seg")
    print("El client finalitza aquí. El monitor s'encarregarà de calcular les mètriques de processament.")

    client.connection.close()

if __name__ == "__main__":
    main()