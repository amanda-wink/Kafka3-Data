from time import sleep
from json import dumps
from kafka import KafkaProducer
import time
import random
import os
from sqlalchemy import create_engine, Table
from sqlalchemy.ext.declarative import declarative_base

user = os.getenv('MYSQL_user')
pw = os.getenv('MYSQL')
str_sql = 'mysql+mysqlconnector://' + user + ':' + pw + '@localhost/ZipBank'
engine = create_engine(str_sql)
Base = declarative_base(bind=engine)

class Producer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda m: dumps(m).encode('ascii'))
        self.customer = []
        self.customer_id = 0

    def emit(self, cust=55, type="dep"):
        data = {'custid' : self.customer_id,
            'createdate': int(time.time()),
            'fname': self.getFname(),
            'lname': self.getLname()
            }
        return data

    def getFname(self):
        fname = ['Hunter','Melanie', 'Tyler', 'Devin', 'Kiki']
        f = random.randint(0, 4)
        return fname[f]

    def getLname(self):
        lname = ['Grainger', 'Snape', 'Lovegood', 'Dumbledore', 'Weasley']
        l = random.randint(0, 4)
        return lname[l]

    def CustDb(self):
        with engine.connect() as connection:
            cust = connection.execute("select * from person")
            cust_list = cust.fetchall()
            for row in range(len(cust_list)):
                val = cust_list[row][0]
                self.customer.append(val)

    def getCustid(self):
        if len(self.customer) > 0:
            self.customer_id = max(self.customer) + 1

    def generateRandomXactions(self, n=1000):
        self.CustDb()
        self.getCustid()
        for _ in range(n):
            data = self.emit()
            print('sent', data)
            self.producer.send('bank-customer-new', value=data)
            self.customer_id += 1
            sleep(1)



if __name__ == "__main__":
    p = Producer()
    p.generateRandomXactions(n=5)