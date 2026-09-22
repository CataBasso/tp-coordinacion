import os
import logging
import threading
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.control_input = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_control_{ID}"]
        )

        self.control_output_exchanges = []
        for i in range(SUM_AMOUNT):
            control_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, SUM_CONTROL_EXCHANGE, [f"{SUM_PREFIX}_control_{i}"]
            )
            self.control_output_exchanges.append(control_output_exchange)

        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)

        self.amount_by_client = {}
        self.lock = threading.Lock()
        self.condition = threading.Condition(self.lock)
        self.processing_clients = set()

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Processing data for client {client_id}")
        with self.lock:
            amount_by_fruit = self.amount_by_client.setdefault(client_id, {})
            amount_by_fruit[fruit] = amount_by_fruit.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, int(amount))

    def _broadcast_eof(self, client_id):
        logging.info(f"Client {client_id} finished, notifying every Sum replica")
        for control_output_exchange in self.control_output_exchanges:
            control_output_exchange.send(
                message_protocol.internal.serialize([client_id])
            )

    # Asigna de forma determinística cada fruta a una réplica de aggregation.
    def _partition_for_fruit(self, fruit):
        accumulated = 0
        for char in fruit:
            accumulated = (accumulated * 31 + ord(char)) % AGGREGATION_AMOUNT
        return accumulated

    def _flush_client(self, client_id):
        logging.info(f"Flushing client {client_id} (sum replica {ID})")
        with self.lock:
            amount_by_fruit = self.amount_by_client.pop(client_id, {})

        for final_fruit_item in amount_by_fruit.values():
            partition = self._partition_for_fruit(final_fruit_item.fruit)
            self.data_output_exchanges[partition].send(
                message_protocol.internal.serialize(
                    [client_id, final_fruit_item.fruit, final_fruit_item.amount, ID]
                )
            )

        logging.info(f"Broadcasting EOF message for client {client_id}")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([client_id, ID]))

    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            client_id = fields[0]
            with self.condition:
                self.processing_clients.add(client_id)
            
            try:
                self._process_data(*fields)
                ack()
            finally:
                with self.condition:
                    self.processing_clients.remove(client_id)
                    self.condition.notify_all()
        else:
            self._broadcast_eof(fields[0])
            ack()

    def process_control_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]
        with self.condition:
            while client_id in self.processing_clients:
                self.condition.wait()

        self._flush_client(client_id)
        ack()

    def start(self):
        control_thread = threading.Thread(
            target=self.control_input.start_consuming,
            args=(self.process_control_message,),
            daemon=True,
        )
        control_thread.start()
        self.input_queue.start_consuming(self.process_data_messsage)

    def stop(self):
        for resource in (self.input_queue, self.control_input):
            try:
                resource.stop_consuming()
            except middleware.MessageMiddlewareDisconnectedError as e:
                logging.error(f"Error stopping sum consumer: {e}")

    def close(self):
        for resource in (self.input_queue, self.control_input):
            try:
                resource.close()
            except middleware.MessageMiddlewareCloseError as e:
                logging.error(f"Error closing sum resource: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()

    def handle_sigterm(signum, frame):
        logging.info("SIGTERM received, stopping sum gracefully")
        sum_filter.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        sum_filter.start()
    finally:
        sum_filter.close()

    return 0


if __name__ == "__main__":
    main()