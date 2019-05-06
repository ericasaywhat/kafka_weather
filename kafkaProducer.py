from time import sleep, gmtime, strftime
from json import dumps, loads
from kafka import KafkaProducer
import config
import requests


class Producer:
    def __init__(self, topic_name, city_id, app_id):
        self.producer = self.init_producer()
        self.test_data = self.init_test_data("testData.txt")
        self.city_id = city_id
        self.app_id = app_id
        self.topic_name = topic_name

    def init_producer(self):
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))
        return producer

    def init_test_data(self, filename):
        with open(filename, "r+") as file:
            json_data = file.read()
            json_data = loads(json_data)
            return json_data["list"][0]["wind"]

    def get_main_data(self, units="imperial"):
        data = requests.get("http://api.openweathermap.org/data/2.5/forecast?id={}&APPID={}&units={}".format(self.city_id, self.app_id, units))
        json_data = loads(data.text)

        return json_data["list"][0]["wind"]

    def send_data(self, data):
        data = {strftime("%Y-%m-%d %H:%M:%S", gmtime()): data}
        print (data)
        self.producer.send(self.topic_name, value=data)

if __name__ == "__main__":
    producer = Producer('windTest', '4945055', config.api_key)
    while True:
        data = producer.get_main_data()
        producer.send_data(data)
        sleep(60)       #sleeps for 60 seconds; weather data is only updated once every 10 minutes
