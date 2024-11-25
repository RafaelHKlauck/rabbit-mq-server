import asyncio
import json
import os
import threading
from dotenv import load_dotenv

import pika
import websockets

from endpoints import post_news, run_flask
from managers import rabbit_manager
import utils

# Mapeia clientes conectados ao WebSocket
online_clients = {}

load_dotenv()
connection_url = os.getenv('RABBIT_URL')

def consume_messages(queue_name, client_id, stop_event):
  connection = pika.BlockingConnection(pika.ConnectionParameters(connection_url))
  channel = connection.channel()

  try:
    # Callback executado ao receber uma mensagem
    def on_message(ch, method, properties, body):
      if stop_event.is_set():
          return
      if client_id in online_clients:
        websocket, _ = online_clients[client_id]
        if websocket.open:
          received_message = body.decode('utf-8')
          loaded_message = json.loads(received_message)

          header = loaded_message.get('header', '')
          topic = loaded_message.get('topic', '')
          message = loaded_message.get('message', '')
          date = loaded_message.get('date', '')      
          post_news(header, topic, message, date, client_id)

          print(f"Enviando mensagem para cliente {client_id}: {received_message}")
          # Envia a mensagem para o cliente via WebSocket
          asyncio.run(websocket.send(received_message)) 
          # Confirma a mensagem manualmente. Foi desativado o auto_ack para garantir que a mensagem seja processada corretamente
          ch.basic_ack(delivery_tag=method.delivery_tag)  
        else:
          print(f"WebSocket para cliente {client_id} está fechado.")
      else:
        print(f"Cliente {client_id} desconectado. Ignorando mensagem.")

    # Configura o consumidor para a fila do cliente
    channel.basic_consume(queue=queue_name, on_message_callback=on_message, auto_ack=False)

    print(f"Iniciando consumo de mensagens para cliente {client_id}.")
    while not stop_event.is_set():
      # Aguarda a chegada de mensagens
      channel.connection.process_data_events(time_limit=1)
  except Exception as e:
    print(f"Erro ao consumir mensagens para cliente {client_id}: {e}")
  finally:
    channel.close()
    connection.close()
    print(f"Conexão RabbitMQ para cliente {client_id} encerrada.")


async def handle_client(websocket, path):
  client_id = path.strip("/")
  topics = utils.get_user_topics(client_id)

  # Cria canal e fila personalizada para o cliente
  connection, channel, queue_name = rabbit_manager.get_or_create_channel(client_id, topics)
  online_clients[client_id] = (websocket, queue_name)

  print(f"Cliente {client_id} conectado aos tópicos: {topics}")

  # Evento para sinalizar a parada do consumidor
  stop_event = threading.Event()

  # Inicia o consumo de mensagens para este cliente em uma thread separada
  consume_thread = threading.Thread(target=consume_messages, args=(queue_name, client_id, stop_event))
  consume_thread.daemon = True
  consume_thread.start()

  try:
    # Aguarda o fechamento do WebSocket
    await websocket.wait_closed()
  except websockets.exceptions.ConnectionClosed as e:
    print(f"Cliente {client_id} desconectado inesperadamente: {e}")
  finally:
    # Sinaliza a parada e aguarda a thread de consumo
    stop_event.set()
    consume_thread.join()

    # Remove o cliente e encerra a conexão RabbitMQ
    rabbit_manager.close_connection(client_id)
    print(f"Removendo cliente {client_id} da lista de conectados.")
    if client_id in online_clients:
      del online_clients[client_id]


async def main():
  # Inicia o Flask em um thread separado
  flask_thread = threading.Thread(target=run_flask)
  flask_thread.daemon = True  # Permite encerrar o thread ao finalizar o programa
  flask_thread.start()

  # Inicia o servidor WebSocket
  server = await websockets.serve(handle_client, "localhost", 8765, ping_interval=30, ping_timeout=60)
  print("Servidor WebSocket iniciado.")
  await server.wait_closed()


asyncio.run(main())
