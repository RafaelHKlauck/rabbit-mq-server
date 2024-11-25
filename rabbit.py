import os
from dotenv import load_dotenv
import pika
import utils

load_dotenv()
connection_url = os.getenv('RABBIT_URL')

class RabbitMQConnectionManager:
  _instance = None

  def __new__(cls, *args, **kwargs):
    # Singleton -> Apenas uma instância da classe é criada
    if not cls._instance:
        cls._instance = super(RabbitMQConnectionManager, cls).__new__(cls, *args, **kwargs)
        cls._instance.connections = {}
    return cls._instance
  
  def open_connection(self):
    # Cria uma conexão com o RabbitMQ
    return pika.BlockingConnection(pika.ConnectionParameters(connection_url))
      
  def get_or_create_channel(self, client_id, topics=None):
    if client_id not in self.connections:
      # Cria a conexão e o canal
      connection = self.open_connection()
      channel = connection.channel()
      # Declaração das exchanges
      channel.exchange_declare(exchange='news', exchange_type='topic', durable=True)
      channel.exchange_declare(exchange="broadcast", exchange_type="fanout", durable=True)

      # Criar fila personalizada para o cliente
      queue_name = f"{client_id}_queue"
      try:
        # Tenta declarar a fila, se ela já existir e for durável, não há problema
        channel.queue_declare(queue=queue_name, durable=True, passive=True)
      except pika.exceptions.ChannelClosedByBroker:
        # Se a fila não existir, ela é criada. Isso é necessário para garantir que a fila seja durável, caso contrário, ela não será persistida
        # e quando conectada novamente, a fila será perdida, juntamente com as mensagens não lidas
        connection = self.open_connection()
        channel = connection.channel()
        channel.queue_declare(queue=queue_name, durable=True)
      # Associa a fila à exchange de broadcast
      channel.queue_bind(exchange="broadcast", queue=queue_name)

      # Associa os tópicos, se fornecidos
      if topics:
        for topic in topics:
          routing_key = utils.build_routing_key(topic)
          # Associa a fila à exchange de notícias com os tópicos interessados
          channel.queue_bind(exchange='news', queue=queue_name, routing_key=routing_key)
      self.connections[client_id] = (connection, channel, queue_name)
    return self.connections[client_id]

  def close_connection(self, client_id):
    if client_id in self.connections:
      connection, channel, _ = self.connections.pop(client_id)
      connection.close()
