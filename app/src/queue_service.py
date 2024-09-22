import pika
import json
import logging
import threading

#logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

def connect_rabbitmq():
    """Establish a connection to RabbitMQ and return the connection and channel."""
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='plans', durable=True)
    return connection, channel

def publish_message(message):
    """Publish a message to the RabbitMQ queue."""
    connection, channel = connect_rabbitmq()
    channel.basic_publish(
        exchange='',
        routing_key='plans',
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # Make message persistent
        )
    )
    logger.info("Published message to RabbitMQ")
    connection.close()

def consume_messages(callback):
    """Consume messages from the RabbitMQ queue and process them using the provided callback."""
    connection, channel = connect_rabbitmq()

    def callback_wrapper(ch, method, properties, body):
        message = json.loads(body)
        callback(message)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue='plans', on_message_callback=callback_wrapper, auto_ack=False)
    logger.info("Consuming messages from RabbitMQ")
    channel.start_consuming()


def start_consumer_thread(callback):
    """Start the RabbitMQ consumer in a separate thread."""
    consumer_thread = threading.Thread(target=consume_messages, args=(callback,))
    #consumer_thread.daemon = True  # Allows the thread to exit when the main program exits
    consumer_thread.start()
    logger.info("Started RabbitMQ consumer thread")
