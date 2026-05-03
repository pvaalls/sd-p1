import psycopg2
import pika

# config PostgreSQL
DB_HOST = "localhost"
DB_NAME = "concerts"
DB_USER = "postgres"
DB_PASS = "admin"

# config RabbitMQ
RABBIT_HOST = 'localhost'
QUEUE_NAME = 'cues_compra_num'
METRICS_QUEUE = 'cues_metriques_num'

def reset_postgresql():
    try:
        # Connectar a la base de dades
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cursor = conn.cursor()
        
        # Crear la taula si no existeix
        # Utilitzem VARCHAR per permetre IDs de seients tipus "VIP-1", "A-12", etc.
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS seients_venuts (
                seat_id VARCHAR(50) PRIMARY KEY,
                client_id VARCHAR(50) NOT NULL
            );
        """)
        
        # Buidar la taula. 
        cursor.execute("TRUNCATE TABLE seients_venuts;")
        
        conn.commit()
        cursor.close()
        conn.close()
        print("[-] PostgreSQL: Taula 'seients_venuts' verificada i buidada correctament.")
    except Exception as e:
        print(f"[x] Error connectant a PostgreSQL: {e}")

def purge_rabbitmq():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
        channel = connection.channel()
        
        # Declarar les cues persistents abans de purgar per evitar errors
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.queue_declare(queue=METRICS_QUEUE, durable=True)
        
        # Buidar missatges antics
        channel.queue_purge(queue=QUEUE_NAME)
        channel.queue_purge(queue=METRICS_QUEUE)
        
        connection.close()
        print("[-] RabbitMQ: Cues persistents 'num' purgades correctament.")
    except Exception as e:
        print(f"[x] Error connectant a RabbitMQ: {e}")

def main():
    print("Iniciant reset de l'entorn per a Entrades Numerades...")
    reset_postgresql()
    purge_rabbitmq()
    print("Entorn preparat per a un nou benchmark de numerades.")

if __name__ == "__main__":
    main()