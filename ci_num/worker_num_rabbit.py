import pika
import json
import time
import psycopg2

# config RabbitMQ
RABBIT_HOST = 'localhost'
QUEUE_NAME = 'cues_compra_num'
METRICS_QUEUE = 'cues_metriques_num'

# config PostgreSQL
DB_HOST = "localhost"
DB_NAME = "concerts"
DB_USER = "postgres"
DB_PASS = "admin"

# Obrir la connexió global a PostgreSQL
try:
    db_conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
    
    # Desactivar l'escriptura síncrona al disc per millorar operacions per segon
    cursor_config = db_conn.cursor()
    cursor_config.execute("SET synchronous_commit = OFF;")
    cursor_config.close()
except Exception as e:
    print(f"Error fatal connectant a PostgreSQL: {e}")
    exit(1)

def on_request(ch, method, props, body):
    """
    Callback que processa la validació de cada seient contra PostgreSQL.
    """
    dades = json.loads(body)
    client_id = dades.get('client_id')
    request_id = dades.get('request_id')
    seat_id = dades.get('seat_id')

    compra_ok = False

    # Lògica de base de dades per a concurrència 
    try:
        cursor = db_conn.cursor()
        
        # S'intenta inserir el seient. Si ja existeix, es descarta l'operació i no retorna res.
        cursor.execute("""
            INSERT INTO seients_venuts (seat_id, client_id) 
            VALUES (%s, %s) 
            ON CONFLICT (seat_id) DO NOTHING 
            RETURNING seat_id;
        """, (seat_id, client_id))
        
        resultat = cursor.fetchone()
        
        # Si la consulta retorna el seat_id, la inserció ha tingut èxit
        if resultat:
            compra_ok = True
            
        db_conn.commit()
        cursor.close()
            
    except Exception as e:
        print(f"Error a PostgreSQL: {e}")
        db_conn.rollback() # Restablir l'estat de la transacció en cas d'error
        compra_ok = False

    # Preparar l'esdeveniment per al monitor
    esdeveniment = {
        'tipus': 'NUMERADA',
        'client_id': client_id,
        'request_id': request_id,
        'seat_id': seat_id,
        'success': compra_ok,
        'timestamp': time.time()
    }

    # Enviar a la cua de mètriques
    ch.basic_publish(
        exchange='',
        routing_key=METRICS_QUEUE,
        body=json.dumps(esdeveniment),
        properties=pika.BasicProperties(
            delivery_mode=2, # Assegurar que la mètrica és persistent
        )
    )

    #  Confirmar processament a RabbitMQ
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
    channel = connection.channel()

    # Declarar ambdues cues com a persistents
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_declare(queue=METRICS_QUEUE, durable=True)

    channel.basic_qos(prefetch_count=100)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_request)

    print("Worker Numerat PostgreSQL esperant peticions...")
    
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nAturant worker numerat...")
        db_conn.close() # Tancar connexió de forma segura

if __name__ == '__main__':
    main()