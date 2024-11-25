import asyncio
import json
from flask import Flask, request, jsonify
import psycopg2
from flask_cors import CORS
import pika
from managers import rabbit_manager

from shared import broadcast_message
from utils import connect_to_db
import utils

app = Flask(__name__)
CORS(app)

global_user_id = None

# Inicia o servidor Flask
def run_flask():
  app.run(host='localhost', port=5001, debug=False)

# Rota para login
@app.route('/login', methods=['POST'])
def login():
  global global_user_id
  connection = connect_to_db()
  cursor = connection.cursor()
  username = request.json.get('username')
  if not username:
    return jsonify({"error": "O campo 'username' é obrigatório"}), 400
  cursor.execute('SELECT * FROM users WHERE username = %s', (username,))  # <---- Observe a vírgula
  result = cursor.fetchone()
  user_id = result[0]
  user_name = result[1]
  user_type = result[3]
  global_user_id = user_id
  user = {
    'id': user_id,
    'username': user_name,
    'type': user_type
  }
  return jsonify(user), 200

# Função para obter todos os tópicos
def get_all_topics():
  connection = connect_to_db()
  cursor = connection.cursor()

  cursor.execute('SELECT id, name FROM topics')
  topics = [{'id': row[0], 'name': row[1]} for row in cursor.fetchall()]
  cursor.close()
  connection.close()

  return topics

# Rota para obter todos os tópicos
@app.route('/all-topics')
def get_all_topics():
  topics = get_all_topics()
  return jsonify(topics)

# Função para obter tópicos por ID
def get_topics_by_id(id: int | None):
  connection = connect_to_db()
  cursor = connection.cursor()

  if id is None:
    cursor.execute('SELECT id, name FROM topics WHERE parent_topic_id IS NULL')
  else:
    cursor.execute('SELECT id, name FROM topics WHERE parent_topic_id = %s', (id,))

  topics = [{'id': row[0], 'name': row[1]} for row in cursor.fetchall()]
  cursor.close()
  connection.close()

  return topics

# Rota para obter tópicos
@app.route('/topics')
def get_topics():
  topics = get_topics_by_id(None)
  response = jsonify(topics)
  return response

# Rota para obter sub-tópicos
@app.route('/topics/<int:id>')
def get_subtopics(id):
  topics = get_topics_by_id(id)
  return jsonify(topics)

# Rota para enviar mensagem
@app.route('/write-news', methods=['POST'])
def write_message():
  data = request.json
  client_id = data.get('client_id')
  header = data.get('header')
  topic = data.get('topic')
  message = data.get('message')
  date = data.get('date')
  # Canal de comunicação com o RabbitMQ
  connection, channel, _ = rabbit_manager.get_or_create_channel(client_id)

  if not client_id:
    return jsonify({"error": "O campo 'client_id' é obrigatório"}), 400
  if not header:
    return jsonify({"error": "O campo 'header' é obrigatório"}), 400
  if not topic:
    return jsonify({"error": "O campo 'topic' é obrigatório"}), 400
  if not message:
    return jsonify({"error": "O campo 'message' é obrigatório"}), 400
  if not date:
    return jsonify({"error": "O campo 'date' é obrigatório"}), 400
  print(topic, '<<<')
  body = {
    'header': header,
    'topic': topic,
    'message': message,
    'date': date
  }
  channel.basic_publish(exchange='news', routing_key=topic, body=json.dumps(body))
  return jsonify({"success": "Mensagem enviada com sucesso"}), 200

# Rota para obter notícias
@app.route('/news', methods=['GET'])
def get_news():
  connection = connect_to_db()
  cursor = connection.cursor()
  
  cursor.execute('SELECT * FROM news WHERE user_id = %s ORDER BY date ASC', (global_user_id,))
  news = [{'id': row[0], 'header': row[1], 'topic': row[2], 'message': row[3], 'date': row[4]} for row in cursor.fetchall()]
  cursor.close()
  connection.close()
  return jsonify(news)

# Função para postar notícias(usada no websocket.py)
def post_news(header, topic, message, date, client_id):
  connection = connect_to_db()
  cursor = connection.cursor()
  cursor.execute('INSERT INTO news (header, topic, message, date, user_id) VALUES (%s, %s, %s, %s, %s)', (header, topic, message, date, client_id))
  connection.commit()
  cursor.close()
  connection.close()

# Rota enviar broadcast
@app.route('/broadcast', methods=['POST'])
def send_broadcast():
  data = request.json
  header = data.get("header")
  message = data.get("message")
  date = data.get("date")

  if not header or not message or not date:
    return jsonify({"error": "Campos 'header', 'message' e 'date' são obrigatórios"}), 400

  broadcast_message(header, message, date)
  
  return jsonify({"success": "Mensagem de broadcast enviada com sucesso"}), 200

# Rota para obter tópicos do usuário
@app.route('/user-topics', methods=['GET'])
def get_user_topics():
  global global_user_id
  topics = utils.get_user_topics(global_user_id)
  return jsonify(topics)