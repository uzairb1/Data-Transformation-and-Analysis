import pika, os.path, sys, errno
from csv import writer
import unittest

def file_exists(file_name):
    check_file = os.path.isfile(file_name)
    if check_file == 'False':
         raise FileNotFoundError(
             errno.ENOENT, os.strerror(errno.ENOENT), file_name)

def test_conn(channel):

        properties = pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        )

        channel.queue_declare(queue='', durable=True)
        print("test successful, please press STRG+C")

        channel.basic_consume(queue='', on_message_callback=callback, auto_ack=True)

        channel.start_consuming()

def callback(self, ch: pika.adapters.blocking_connection.BlockingChannel, method, _, body):
    print(body)

    ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == '__main__':
    file_exists("..//de_challenge_sample_data.csv")
    try:
        credentials = pika.PlainCredentials("guest","guest")
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host="localhost",credentials= credentials))
        channel = connection.channel()

        channel.exchange_declare(exchange='globalen_anzahl', exchange_type='fanout')
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        result = channel.queue_declare(queue='', exclusive=True)
        consumer = test_conn(connection.channel())
    except KeyboardInterrupt:
        print('Finished')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)