import pika
import uuid
import json
import time

#config
RABBIT_HOST = 'localhost'
QUEUE_NAME = 'cues_compra_num'
FITXER_BENCHMARK = "./data/benchmark_numbered_60000.txt" 

class TicketClientRPC:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
        self.channel = self.connection.channel()

        result = self.channel.queue_declare(queue='', exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(
            queue=self.callback_queue,
            on_message_callback=self.on_response,
            auto_ack=True
        )

        self.respostes_rebudes = 0
        self.total_peticions = 0
        self.exitoses = 0

    def on_response(self, ch, method, props, body):
        dades = json.loads(body)
        if dades.get('success'):
            self.exitoses += 1
        self.respostes_rebudes += 1

    def enviar_peticio(self, client_id, seat_id, request_id):
        corr_id = str(uuid.uuid4())
        missatge = {
            'client_id': client_id,
            'seat_id': seat_id,
            'request_id': request_id
        }

        self.channel.basic_publish(
            exchange='',
            routing_key=QUEUE_NAME,
            properties=pika.BasicProperties(
                reply_to=self.callback_queue,
                correlation_id=corr_id,
            ),
            body=json.dumps(missatge)
        )
        self.total_peticions += 1

def main():
    client_rpc = TicketClientRPC()

    try:
        with open(FITXER_BENCHMARK, 'r') as f:
            lines = f.readlines()
    except FileNotFoundError:
        print(f"Error: No es troba el fitxer {FITXER_BENCHMARK}")
        return

    print("Enviant peticions numerades a RabbitMQ...")
    start_time = time.time()

    #llegir el format del fitxer
    for i, line in enumerate(lines):
        parts = line.strip().split()
        if len(parts) == 4 and parts[0] == 'BUY':
            client_id = parts[1]
            seat_id = parts[2]
            request_id = parts[3]

            client_rpc.enviar_peticio(client_id, seat_id, request_id)

    print("Peticions enviades. Esperant respostes...")

    while client_rpc.respostes_rebudes < client_rpc.total_peticions:
        client_rpc.connection.process_data_events(time_limit=1)
        print(f"\rProgrés: {client_rpc.respostes_rebudes} / {client_rpc.total_peticions} respostes rebudes...", end="")

    print("\n")

    end_time = time.time()
    temps_total = end_time - start_time
    throughput = client_rpc.total_peticions / temps_total if temps_total > 0 else 0

    print(f"Benchmark finalitzat en {temps_total:.4f} segons.")
    print(f"Operacions exitoses: {client_rpc.exitoses}/{client_rpc.total_peticions}")
    print(f"Throughput: {throughput:.2f} ops/seg")

    client_rpc.connection.close()

if __name__ == "__main__":
    main()
