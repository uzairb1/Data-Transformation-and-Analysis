import pika, sys, os, json
import pandas as pd
from csv import writer

def append_list_as_row(file_name, list_of_elem):
     with open(file_name, 'a+', newline='') as write_obj:
        # Create a writer object from csv module
        csv_writer = writer(write_obj)
        # Add contents of list as last row in the csv file
        csv_writer.writerow(list_of_elem)

def main():
    credentials = pika.PlainCredentials("guest","guest")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="localhost",credentials= credentials))
    channel = connection.channel()

    channel.exchange_declare(exchange='globalen_anzahl', exchange_type='fanout')

    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='globalen_anzahl', queue=queue_name)

    print(' [*] Waiting for logs. To exit press CTRL+C')

    def callback(ch, method, properties, body):
        body_list = json.loads(body)

        body_dict=json.loads(body_list[1])
        #pprint(body_dict['title'])
        
        df = pd.DataFrame(body_dict)
        df = df.join(pd.json_normalize(df.pop('Unnamed: 0')))
        df['wiki'] = df['wiki'].astype(str)

        de_df = df.loc[df['wiki'] == 'dewiki']

        count_global = str(len(df))
        count_deutsch = str(len(de_df))

        minute =  str(body_list[0]).replace('group_','')
        row_contents = ["anzahl bearbeitungen in minute "+minute +":"+ count_global]
        append_list_as_row('global_edits.csv', row_contents)#type:ignore
        row_contents = ["anzahl bearbeitungen in minute "+minute +":"+ count_deutsch]
        append_list_as_row('deutsch_edits.csv', row_contents)#type:ignore
        print("Global Count: "+count_global)
        print("German Count: "+count_deutsch)
        

    channel.basic_consume(
        queue=queue_name, on_message_callback=callback, auto_ack=True)

    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)