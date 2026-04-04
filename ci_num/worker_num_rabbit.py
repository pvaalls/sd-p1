import pika
import redis
import json

#config
RABBIT_HOST = 'localhost'
REDIS_HOST = 'localhost'
QUEUE_NAME = 'cues_compra_num'

#connec a Redis
redisserver = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

def on_request(ch, method, props, body):
    """
    Callback que processa la validació de cada seient.
    """
    #Es deserialitza el missatge
    dades = json.loads(body)
    client_id = dades.get('client_id')
    request_id = dades.get('request_id')
    seat_id = dades.get('seat_id')

    #bd
    try:
        #hsetnx retorna 1 si s'insereix -seient lliure-, 0 si ja existeix -seient ocupat-
        resultat = redisserver.hsetnx("seients_venuts", seat_id, client_id)
        
        if resultat == 1:
            compra_ok = True
        else:
            compra_ok = False
            
    except Exception as e:
        print(f"Error a Redis: {e}")
        compra_ok = False

    #es prepara la resposta
    resposta = {
        'client_id': client_id,
        'request_id': request_id,
        'seat_id': seat_id,
        'success': compra_ok
    }

    #s'envia la resposta a la cua del client
    ch.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        properties=pika.BasicProperties(
            correlation_id=props.correlation_id,
        ),
        body=json.dumps(resposta)
    )

    # ack del msg
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME)

    #serveix per optimitzar rendiment, quants msg enviem a cada worker cada vegada
    channel.basic_qos(prefetch_count=100)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_request)

    print("Worker Numerat RabbitMQ esperant peticions...")
    channel.start_consuming()

if __name__ == '__main__':
    main()  