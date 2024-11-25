import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

db_host = os.getenv('DATABASE_HOST')
db_port = os.getenv('DATABASE_PORT')
db_name = os.getenv('DATABASE_NAME')
db_user = os.getenv('DATABASE_USER')
db_password = os.getenv('DATABASE_PASSWORD')

def connect_to_db():
  return psycopg2.connect(
    dbname=db_name,
    user=db_user,
    password=db_password,
    host=db_host,
    port=db_port
  )

def get_user_topics(id):
  """Consulta os tópicos associados a um usuário no banco de dados."""
  connection = connect_to_db()
  cursor = connection.cursor()

  # Recuperar os tópicos associados ao usuário
  cursor.execute('''
  SELECT t.name
  FROM topics t
  JOIN user_topics ut ON t.id = ut.topic_id
  JOIN users u ON ut.user_id = u.id
  WHERE u.id = %s
  ''', (id,))
  
  topics = [row[0] for row in cursor.fetchall()]
  cursor.close()
  connection.close()

  return topics

def build_routing_key(topic_name):
  """Determina a `routing_key` apropriada para o tópico."""
  connection = connect_to_db()
  cursor = connection.cursor()
  
  # Verifica se o tópico possui sub-tópicos
  cursor.execute('''
  SELECT COUNT(*) 
  FROM topics 
  WHERE parent_topic_id = (SELECT id FROM topics WHERE name = %s)
  ''', (topic_name,))
  
  has_subtopics = cursor.fetchone()[0] > 0
  cursor.close()
  connection.close()
  
  # Adiciona `.#` se o tópico tiver sub-tópicos
  if has_subtopics:
    return f"#.{topic_name}.#"
  return f"#.{topic_name}"