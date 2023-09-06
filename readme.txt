Bitte erst python und docker-desktop installieren lassen
1.To install the dependencies run the following command
- pip install -r requirements.txt

2.To start the rabbitmq service
- docker-compose up

3.To create the schema run
- python create_schema.py

5.To run the producer
- python kafka-producers/producer.py

6.To run the consumer/s
- python kafka-consumers/anzahl_deutsch_consumers.py
- python kafka-consumers/anzahl_global_comsumer.py

After running all of these commands