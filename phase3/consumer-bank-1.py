from kafka import KafkaConsumer, TopicPartition
from json import loads
from sqlalchemy import create_engine, Table, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
import os

user = os.getenv('MYSQL_user')
pw = os.getenv('MYSQL')
str_sql = 'mysql+mysqlconnector://' + user + ':' + pw + '@localhost/ZipBank'
engine = create_engine(str_sql)
Base = declarative_base(bind=engine)

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionaries
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current balance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.

        #Go back to the readme.

    def handleMessages(self):
        self.consumer.assign([TopicPartition('bank-customer-events2', 1)])
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            with engine.connect() as connection:
                connection.execute("insert into transaction_bank1 (bankid, custid, type, date, amt) values(%s, %s, %s, %s, %s)", (message['bankid'], message['custid'], message['type'], message['date'], message['amt']))
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)

class Transaction(Base):
    __tablename__ = 'transaction_bank1'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    bankid = Column(Integer)
    custid = Column(Integer)
    type = Column(String(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)

if __name__ == "__main__":
    Base.metadata.create_all(engine)
    c = XactionConsumer()
    c.handleMessages()
