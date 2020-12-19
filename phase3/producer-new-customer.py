from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
import random

class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: dumps(m).encode('ascii'))
        self.customer_id = 50

    def emit(self, cust=55, type="dep"):
        data = {'custid' : self.customer_id,
            'createdate': int(time.time()),
            'fname': self.getFname(),
            'lname': self.getLname()
            }
        return data
    """
    def getCustid(self):
        self.customer_id += 1
        print(self.customer_id)
        return self.customer_id
    """

    def getFname(self):
        fname = ['Hunter','Melanie', 'Tyler', 'Devin', 'Kiki']
        f = random.randint(0, 4)
        return fname[f]

    def getLname(self):
        lname = ['Grainger', 'Snape', 'Lovegood', 'Dumbledore', 'Weasley']
        l = random.randint(0, 4)
        return lname[l]


    def generateRandomXactions(self, n=1000):
        for _ in range(n):
            data = self.emit()
            print('sent', data)
            self.producer.send('bank-customer-new', value=data)
            self.customer_id += 1
            sleep(1)

if __name__ == "__main__":
    p = Producer()
    p.generateRandomXactions(n=5)