import threading
import logging as log
import time
import multiprocessing
import os

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError


class Producer(threading.Thread):
    """
    Kafka produce class.
    This class send data to kafka cluster using thread
    Producer should be connect kafka broker for send data
    """

    def __init__(self, filepath):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.broker_servers = ["localhost:9092"]
        self.logPath = "/Users/whitexozu/dev/logs/kafka/edrlog.log"
        self.topic = "test"

    def stop(self):
        self.stop_event.set()

    def run(self):
        producer = KafkaProducer(bootstrap_servers=self.broker_servers)

        with open(self.logPath) as fd:
            allData = fd.readlines()

        i = 1
        for eachData in allData:
            producer.send(self.topic, eachData)
            i = i + 1
            if i == 30:
                break

        producer.close()
        print("Send {} datas to producer".format(i))
        # print(producer.metrics())


if __name__ == "__main__":
    """
    Main code.
    """
    log.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=log.INFO
    )

    broker_servers = ["localhost:9092"]
    topic = "test"
    producer = KafkaProducer(bootstrap_servers=broker_servers)
    basedir = "/Users/whitexozu/dev/data/test/simple"

    i = 0
    j = 0
    for filename in os.listdir(basedir):
        filepath = basedir + '/' + filename
        try:
            with open(filepath) as fd:
                allData = fd.readlines()
                j = j + 1
        except Exception as e:
            print("[I/O Error] {} ".format(e))
            continue

        print("{} file data sending.({})".format(filename, j))

        for eachData in allData:
            producer.send(topic, eachData)
            i = i + 1

    producer.close()
    print("Send {} datas to producer".format(i))