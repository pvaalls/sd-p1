import pika
import json
import time

# config RabbitMQ
RABBIT_HOST = 'localhost'
METRICS_QUEUE = 'cues_metriques'

class TicketMonitor:
    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
        self.channel = self.connection.channel()
        
        # Assegurem que la cua existeix i és persistent
        self.channel.queue_declare(queue=METRICS_QUEUE, durable=True)

        self.total_processats = 0
        self.exitoses = 0
        self.fallides = 0
        self.start_time = None
        self.last_time = None

        self.channel.basic_consume(
            queue=METRICS_QUEUE,
            on_message_callback=self.on_metric_received,
            auto_ack=True
        )

    def on_metric_received(self, ch, method, props, body):
        esdeveniment = json.loads(body)
        timestamp_actual = esdeveniment['timestamp']

        # Inicialització de variables en la primera iteració
        if self.start_time is None:
            self.start_time = timestamp_actual
            self.last_time = timestamp_actual
        else:
            # S'emmagatzema estrictament el valor mínim i màxim absolut
            if timestamp_actual < self.start_time:
                self.start_time = timestamp_actual
            elif timestamp_actual > self.last_time:
                self.last_time = timestamp_actual

        self.total_processats += 1

        if esdeveniment.get('success'):
            self.exitoses += 1
        else:
            self.fallides += 1

        temps_transcorregut = self.last_time - self.start_time
        throughput = self.total_processats / temps_transcorregut if temps_transcorregut > 0 else 0

        print(f"\r[Monitor] Processats: {self.total_processats} | OK: {self.exitoses} | REBUTJADES: {self.fallides} | Temps: {temps_transcorregut:.2f}s | Rendiment: {throughput:.2f} ops/seg", end="")
        
    def start(self):
        print(f"Monitor escoltant a la cua '{METRICS_QUEUE}'...")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            print("\nMonitor aturat manualment.")
            self.connection.close()

if __name__ == "__main__":
    monitor = TicketMonitor()
    monitor.start()