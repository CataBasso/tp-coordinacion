import os
import logging

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.amount_by_fruit_by_client = {}
        self.finished_sum_by_client = {}

    def _process_data(self, client_id, fruit, amount, sum_id):
        logging.info(f"Processing data message for client {client_id} and sum {sum_id}")
        amount_by_fruit = self.amount_by_fruit_by_client.setdefault(client_id, {})
        amount_by_fruit[fruit] = amount_by_fruit.get(fruit, fruit_item.FruitItem(fruit, 0)) + fruit_item.FruitItem(fruit, amount)

    def _process_eof(self, client_id, sum_id):
        logging.info(f"Received EOF for client {client_id} and sum {sum_id}")

        finished_sums = self.finished_sum_by_client.setdefault(client_id, set())
        finished_sums.add(sum_id)
        if len(finished_sums) < SUM_AMOUNT:
            return

        logging.info(f"All sums finished for client {client_id}")
        amount_by_fruit = self.amount_by_fruit_by_client.pop(client_id, {})
        self.finished_sum_by_client.pop(client_id, None)

        fruit_chunk = sorted(amount_by_fruit.values())[-TOP_SIZE:]
        fruit_chunk.reverse()
        result = [(item.fruit, item.amount) for item in fruit_chunk]
        self.output_queue.send(message_protocol.internal.serialize([client_id, result]))

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 4:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_messsage)


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    aggregation_filter.start()
    return 0


if __name__ == "__main__":
    main()