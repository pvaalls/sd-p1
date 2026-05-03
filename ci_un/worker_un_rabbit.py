import pika
import json
import time
import psycopg2

RABBIT_HOST = 'localhost'
QUEUE_NAME = 'cues_compra'
METRICS_QUEUE = 'cues_metriques'
LIMIT_ENTRADES = 20000

DB_HOST = "localhost"
DB_NAME = "concerts"
DB_USER = "postgres"
DB_PASS = "admin"

# obrim la conexio una sola vegada
try:
    db_conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)

    # Desactivem l'escriptura síncrona al disc només per a aquesta connexió
    cursor_config = db_conn.cursor()
    cursor_config.execute("SET synchronous_commit = OFF;")
    cursor_config.close()

except Exception as e:
    print(f"Error fatal connectant a PostgreSQL: {e}")
    exit(1)

def on_request(ch, method, props, body):
    dades = json.loads(body)
    client_id = dades.get('client_id')
    request_id = dades.get('request_id')

    compra_ok = False

    try:
        # utilitzem la conexio ja oberta
        cursor = db_conn.cursor()
        
        cursor.execute("""
            UPDATE concert_config 
            SET venudes = venudes + 1 
            WHERE id = 1 AND venudes < %s 
            RETURNING venudes;
        """, (LIMIT_ENTRADES,))
        
        resultat = cursor.fetchone()
        if resultat:
            compra_ok = True
        
        db_conn.commit()
        cursor.close()
        
    except Exception as e:
        print(f"Error a PostgreSQL: {e}")
        db_conn.rollback() # fem rollback si error
        compra_ok = False

    esdeveniment = {
        'tipus': 'NO_NUMERADA',
        'client_id': client_id,
        'request_id': request_id,
        'success': compra_ok,
        'timestamp': time.time()
    }

    ch.basic_publish(
        exchange='',
        routing_key=METRICS_QUEUE,
        body=json.dumps(esdeveniment),
        properties=pika.BasicProperties(
            delivery_mode=2,
        )
    )

    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
    channel = connection.channel()
    
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_declare(queue=METRICS_QUEUE, durable=True)

    channel.basic_qos(prefetch_count=100)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_request)
    print("Worker PostgreSQL esperant peticions...")
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("Aturant worker...")
        db_conn.close() # Tanquem la connexió neta en sortir

if __name__ == '__main__':
    main()