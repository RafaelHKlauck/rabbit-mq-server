import json
import os
from dotenv import load_dotenv
import pika

load_dotenv()
connection_url = os.getenv('RABBIT_URL')

def broadcast_message(header, message, date):
  # Conecta-se ao RabbitMQ e envia uma mensagem de broadcast
  connection = pika.BlockingConnection(pika.ConnectionParameters(connection_url))
  channel = connection.channel()

  body = {
    "header": header,
    "message": message,
    "date": date
  }

  # Declaração da exchange de broadcast, do tipo fanout
  channel.exchange_declare(exchange="broadcast", exchange_type="fanout", durable=True)
  # Envia a mensagem para a exchange de broadcast
  channel.basic_publish(exchange="broadcast", routing_key="", body=json.dumps(body))
  print(f"Mensagem de broadcast enviada: {body}")

  connection.close()
