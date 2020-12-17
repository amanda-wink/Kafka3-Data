from kafka import KafkaConsumer, TopicPartition
from json import loads
import os
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from mysql.connector.errors import Error

user = os.getenv('MYSQL_user')
pw = os.getenv('MYSQL')
str_sql = 'mysql+mysqlconnector://' + user + ':' + pw + '@localhost/ZipBank'
engine = create_engine(str_sql)
Base = declarative_base(bind=engine)

class LimitConsumer():

    def __init__(self, limit_value):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        self.tracker = {}
        self.over_limit = set()
        self.limit_value = limit_value


    def TrackLimit(self):
        self.OverLimitDb()
        self.PrintOverLimit()
        for message in self.consumer:
            message = message.value
            print(message)
            self.UpdateBalance(message)
            if self.tracker[message['custid']]  <= self.limit_value:
                if message['custid'] not in self.over_limit:
                    self.over_limit.add(message['custid'])
                print("{} has exceeded the balance limit of {}".format(message['custid'], self.limit_value))
            else:
                if message['custid'] in self.over_limit:
                    self.over_limit.remove(message['custid'])

    def OverLimitDb(self):
        with engine.connect() as connection:
            limit_list = connection.execute(
                "select sign.custid, sum(sign.neg) sum_tran from (select id, custid, type, if(type = 'wth', -amt, amt) neg from transaction) sign group by sign.custid")
            limit_list2 = limit_list.fetchall()
            for row in range(len(limit_list2)):
                self.tracker[limit_list2[row][0]] = limit_list2[row][1]


    def PrintOverLimit(self):
        for key in self.tracker:
            if self.tracker[key] <= self.limit_value:
                self.over_limit.add(key)
        print("Customers over the limit")
        print(self.over_limit)

    def UpdateBalance(self, message):
        amt = message['amt']
        if message['custid'] in self.tracker.keys():
            if message['type'] == 'wth':
                amt = -amt
            self.tracker[message['custid']] += amt
        else:
            if message['type'] == 'wth':
                amt = -amt
            self.tracker[message['cutid']] = amt

if __name__ == "__main__":
    value = -5000
    l = LimitConsumer(value)
    l.TrackLimit()