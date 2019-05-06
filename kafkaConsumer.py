from kafka import KafkaConsumer
from json import loads, dumps


class Consumer:
    def __init__(self, topic_name):
        self.consumer = self.init_consumer(topic_name)
        # self.write_to_file("numtest.txt")

    def init_consumer(self, topic_name):
        consumer = KafkaConsumer(
            topic_name,
             bootstrap_servers=['localhost:9092'],
             auto_offset_reset='earliest',
             enable_auto_commit=True,
             group_id='my-group',
             value_deserializer=lambda x: loads(x.decode('utf-8')))
        return consumer

    def write_to_file(self, filename):
        with open(filename, "w+") as file:
            for message in self.consumer:
                message = message.value
                file.write(dumps(message))
                print('{} added to {}'.format(message,filename))

if __name__ == "__main__":
    consumer = Consumer('windTest')
    while True:
        consumer.write_to_file("needhamWeather.txt")
