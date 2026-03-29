import pika
import redis
import json

#config
RABBIT_HOST = 'localhost'
REDIS_HOST = 'localhost'
QUEUE_NAME = 'cues_compra'
LIMIT_ENTRADES = 20000

#conec redis
redisserver = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)

def on_request(ch, method, props, body):
    """
    Funció callback que s'executa cada cop que arriba un missatge a la cua.
    """
    #Deserialitzar el missatge del client
    dades = json.loads(body)
    client_id = dades.get('client_id')
    request_id = dades.get('request_id')


    try:
        entrades = redisserver.incr("entrades_venudes")
        if entrades <= LIMIT_ENTRADES:
            compra_ok = True
        else:
            compra_ok = False
    except Exception as e:
        print(f"Error a Redis: {e}")
        compra_ok = False

    #Preparar la resposta per al client
    resposta = {
        'client_id': client_id,
        'request_id': request_id,
        'success': compra_ok
    }

    #Envia la resposta a la cua temporal del client
    ch.basic_publish(
        exchange='',
        routing_key=props.reply_to,
        properties=pika.BasicProperties(
            correlation_id=props.correlation_id,
        ),
        body=json.dumps(resposta)
    )

    #Confirmar a rabbit que el missatge s'ha processat correctament
    ch.basic_ack(delivery_tag=method.delivery_tag)

def main():
    #concec rabbit
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
    channel = connection.channel()
    #declarem qua principal
    channel.queue_declare(queue=QUEUE_NAME)

    #Configurar el qualitiy of service, aixo gestiona quantes peticions s'envien a cada worker de cop
    channel.basic_qos(prefetch_count=100)

    #Indicar a rabbit quina funció ha de processar els missatges
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_request)
    print("Worker RabbitMQ esperant peticions...")
    channel.start_consuming()

if __name__ == '__main__':
    main()