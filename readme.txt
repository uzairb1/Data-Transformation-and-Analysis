Bitte erst python und docker-desktop installieren lassen
1.To install the dependencies run the following command
- pip install -r requirements.txt

2.To start the rabbitmq service
- docker-compose up

3.To create the schema run
- python create_schema.py

5.To run the receiver
- python rabbitmq_python/receiver.py

6.To run the producer
- python rabbitmq_python/producer.py
please first run the receiver
After running all of these commands
