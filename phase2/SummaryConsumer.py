from kafka import KafkaConsumer, TopicPartition
from json import loads
from statistics import mean, stdev

class Summary:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        self.mean_dep = "Not enough data"
        self.mean_wth = "Not enough data"
        self.stdev_dep = "Not enough data"
        self.stdev_wth = "Not enough data"
        self.dep = []
        self.wth = []

    def __repr__(self):
        return [self.mean_dep, self.stdev_dep, self.mean_wth, self.stdev_wth]

    def calculate(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            if message['type'] == 'dep':
                self.dep.append(message['amt'])
            if message['type'] == 'wth':
                self.wth.append(message['amt'])
            if len(self.dep) > 1 and len(self.wth) > 1:
                self.mean_dep = round(mean(self.dep), 2)
                self.stdev_dep = round(stdev(self.dep), 4)
            if len(self.wth) > 1:
                self.mean_wth = round(mean(self.wth), 2)
                self.stdev_wth = round(stdev(self.wth), 4)
            print([self.mean_dep, self.stdev_dep, self.mean_wth, self.stdev_wth])


if __name__ == "__main__":
    s = Summary()
    s.calculate()
