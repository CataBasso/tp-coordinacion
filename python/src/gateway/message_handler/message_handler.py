from common import message_protocol


class MessageHandler:

    next_client_id = 0

    def __init__(self):
        self.client_id = MessageHandler.next_client_id
        MessageHandler.next_client_id += 1
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)

        client_id = fields[0]
        fruit_top = fields[1]

        if client_id != self.client_id:
            return []

        return fruit_top
