#!/usr/bin/env python
import dill
import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
from worker import Worker




if __name__ == '__main__':

    worker = Worker(age=30, name="John", profession="Engineer")
    worker_pickle = dill.dumps(worker)

    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            topic = msg.topic()
            key = msg.key()
            value = msg.value()
            
            if key is not None and isinstance(key, bytes):
                key = key.decode('utf-8', errors='ignore')
            
            if value is not None and isinstance(value, bytes):
                try:
                    value = dill.loads(value)
                except dill.UnpicklingError as e:
                    print('ERROR: Failed to unpickle the value: {}'.format(e))
            
            print("Produced event to topic {}: key = {:12} value = {}".format(
                topic, key, value))

    # Produce data by selecting random values from these lists.
    topic = "worker_send_bill" #worker_get_information, worker_validation, worker_send_bill
    last_names = ['ivanov', 'petrov', 'sidorov', 'pechenkin', 'ageev', 'dzerzhisky']
    
    events_and_payments = [{'Стрижка газонов':90102},{'Покраска стен':10500},{'Разгрузка фуры 20 т':15000},{'Клининг':25000}]

    count = 0
    for _ in range(10):

        last_name = choice(last_names)
        events_and_payment = choice(events_and_payments)
        value_bytes = str(events_and_payment).encode('utf-8')

        # producer.produce(topic, value_bytes, last_name, callback=delivery_callback)

        producer.produce(topic, value=worker_pickle, key=last_name.encode('utf-8'), callback=delivery_callback)
        count += 1

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()