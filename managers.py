from rabbit import RabbitMQConnectionManager
# Criação de uma instância do gerenciador de conexões. A partir deste ponto, a instância pode ser importada em qualquer lugar do código
# E será instanciada apenas uma vez
rabbit_manager = RabbitMQConnectionManager()
