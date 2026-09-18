import pika

from .middleware import( 
    MessageMiddlewareQueue, 
    MessageMiddlewareExchange, 
    MessageMiddlewareMessageError, 
    MessageMiddlewareDisconnectedError, 
    MessageMiddlewareCloseError,
)

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self.host = host
        self.queue_name = queue_name
        self._consuming = False

        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)
            self.channel.basic_qos(prefetch_count=1) 
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        try: 
            self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback)
            self._consuming = True 
            self.channel.start_consuming()

            self._consuming = False
        except pika.exceptions.AMQPConnectionError as e:
            self._consuming = False
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            self._consuming = False
            raise MessageMiddlewareMessageError(str(e))
        
    def stop_consuming(self):
        if not self._consuming:
            return

        try:
            self.channel.stop_consuming()
            self._consuming = False
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))

    def send(self, message):
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message, properties=pika.BasicProperties(delivery_mode=2))
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def close(self):
        try:
            if self.channel.is_open:
                self.channel.close()
            if self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        self.host = host
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self._consuming = False
        self._queue_name = None

        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')
            self.channel.basic_qos(prefetch_count=1) 
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def start_consuming(self, on_message_callback):
        def callback(ch, method, properties, body):
            def ack():
                ch.basic_ack(delivery_tag=method.delivery_tag)

            def nack():
                ch.basic_nack(delivery_tag=method.delivery_tag)

            on_message_callback(body, ack, nack)

        try: 
            result = self.channel.queue_declare(queue='', exclusive=True)
            self._queue_name = result.method.queue

            for routing_key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name, queue=self._queue_name, routing_key=routing_key)
            
            self.channel.basic_consume(queue=self._queue_name, on_message_callback=callback)

            self._consuming = True 
            self.channel.start_consuming()

            self._consuming = False
        except pika.exceptions.AMQPConnectionError as e:
            self._consuming = False
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            self._consuming = False
            raise MessageMiddlewareMessageError(str(e))
    
    def stop_consuming(self):
        if not self._consuming:
            return

        try:
            self.channel.stop_consuming()
            self._consuming = False
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
    
    def send(self, message):
        try:
            self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.routing_keys[0], body=message)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(str(e))
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e))

    def close(self):
        try:
            if self.channel.is_open:
                self.channel.close()
            if self.connection.is_open:
                self.connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(str(e))
