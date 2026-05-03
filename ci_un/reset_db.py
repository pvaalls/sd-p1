import psycopg2
import pika

# config PostgreSQL
DB_HOST = "localhost"
DB_NAME = "concerts"
DB_USER = "postgres"
DB_PASS = "admin"

# config RabbitMQ
RABBIT_HOST = 'localhost'
QUEUE_NAME = 'cues_compra'
METRICS_QUEUE = 'cues_metriques'

def reset_postgresql():
    try:
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASS)
        cursor = conn.cursor()
        
        # Crear la taula si no existeix 
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS concert_config (
                id INT PRIMARY KEY,
                venudes INT
            );
        """)
        
        # Inserir la fila inicial a 0 (si és nova) o actualitzar-la a 0 (si ja existia)
        cursor.execute("""
            INSERT INTO concert_config (id, venudes) 
            VALUES (1, 0) 
            ON CONFLICT (id) 
            DO UPDATE SET venudes = 0;
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        print("[-] PostgreSQL: Taula verificada i comptador 'venudes' reiniciat a 0 correctament.")
    except Exception as e:
        print(f"[x] Error connectant a PostgreSQL: {e}")

def purge_rabbitmq():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
        channel = connection.channel()
        
        # Declarar les cues abans de purgar per evitar errors si no existeixen
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.queue_declare(queue=METRICS_QUEUE, durable=True)
        
        channel.queue_purge(queue=QUEUE_NAME)
        channel.queue_purge(queue=METRICS_QUEUE)
        
        connection.close()
        print("[-] RabbitMQ: Cues persistents purgades correctament.")
    except Exception as e:
        print(f"[x] Error connectant a RabbitMQ: {e}")

def main():
    print("Iniciant reset de l'entorn...")
    reset_postgresql()
    purge_rabbitmq()
    print("Entorn preparat per a un nou benchmark.")

if __name__ == "__main__":
    main()