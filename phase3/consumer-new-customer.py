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
        self.consumer = KafkaConsumer('bank-customer-new',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        ## These are two python dictionaries
        # Ledger is the one where all the transaction get posted
        self.customer = {}
        self.customer_list = []


        #Go back to the readme.

    def handleMessages(self):
        self.CustDb()
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.customer[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            if message['custid'] in self.customer_list:
                print("Already a customer")
            else:
                with engine.connect() as connection:
                    connection.execute("insert into person (custid, createdate, fname, lname) values(%s, %s, %s, %s)", (message['custid'], message['createdate'], message['fname'], message['lname']))
            print(self.customer)

    def CustDb(self):
        with engine.connect() as connection:
            cust = connection.execute("select custid from person")
            cust_list = cust.fetchall()
            for row in range(len(cust_list)):
                self.customer_list.append(row)

class Transaction(Base):
    __tablename__ = 'person'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    custid = Column(Integer, primary_key=True)
    createdate = Column(Integer)
    fname = Column(String(50))
    lname = Column(String(50))

if __name__ == "__main__":
    Base.metadata.create_all(engine)
    c = XactionConsumer()
    c.handleMessages()