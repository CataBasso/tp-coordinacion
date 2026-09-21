import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.merged_by_client = {}
        self.received_by_client = {}

    def _merge_partial(self, client_id, fruit_top):
        merged = self.merged_by_client.setdefault(client_id, {})
        for fruit, amount in fruit_top:
            merged[fruit] = merged.get(
                fruit, fruit_item.FruitItem(fruit, 0)
            ) + fruit_item.FruitItem(fruit, amount)

    def process_messsage(self, message, ack, nack):
        client_id, fruit_top = message_protocol.internal.deserialize(message)

        logging.info(f"Received partial top for client {client_id}")
        self._merge_partial(client_id, fruit_top)

        received = self.received_by_client.get(client_id, 0) + 1
        self.received_by_client[client_id] = received

        if received < AGGREGATION_AMOUNT:
            ack()
            return

        logging.info(f"Got every partial top for client {client_id}, joining")
        merged = self.merged_by_client.pop(client_id)
        self.received_by_client.pop(client_id)

        final_chunk = sorted(merged.values())[-TOP_SIZE:]
        final_chunk.reverse()
        final_top = [(item.fruit, item.amount) for item in final_chunk]

        self.output_queue.send(
            message_protocol.internal.serialize([client_id, final_top])
        )
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

    def stop(self):
        try:
            self.input_queue.stop_consuming()
        except middleware.MessageMiddlewareDisconnectedError as e:
            logging.error(f"Error stopping join consumer: {e}")

    def close(self):
        for resource in (self.input_queue, self.output_queue):
            try:
                resource.close()
            except middleware.MessageMiddlewareCloseError as e:
                logging.error(f"Error closing join resource: {e}")

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()

    def handle_sigterm(signum, frame):
        logging.info("SIGTERM received, stopping join gracefully")
        join_filter.stop()

    signal.signal(signal.SIGTERM, handle_sigterm)

    try:
        join_filter.start()
    finally:
        join_filter.close()

    return 0


if __name__ == "__main__":
    main()