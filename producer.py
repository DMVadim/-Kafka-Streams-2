import json
import time
from confluent_kafka import Producer

# Настройки Kafka
conf = {
    'bootstrap.servers': 'localhost:9092'
}

producer = Producer(conf)

# Данные о продуктах
products = [
    {"id": 1, "name": "Product A", "description": "Description of Product A", "price": 55.0},
    {"id": 2, "name": "Product B", "description": "Description of Product B", "price": 60.0},
    {"id": 3, "name": "Product C", "description": "Description of Product C", "price": 65.0},
    {"id": 4, "name": "Product D", "description": "Description of Product D", "price": 7000.0}
]

# Данные о покупках
purchases = [
    {"id": 1, "quantity": 5, "productid": 1},
    {"id": 2, "quantity": 3, "productid": 2},
    {"id": 3, "quantity": 10, "productid": 3},
    {"id": 4, "quantity": 1, "productid": 1},
    {"id": 5, "quantity": 7, "productid": 2},
    {"id": 6, "quantity": 2, "productid": 3},
    {"id": 7, "quantity": 4, "productid": 1},
    {"id": 8, "quantity": 6, "productid": 2},
    {"id": 9, "quantity": 8, "productid": 3},
    {"id": 10, "quantity": 9, "productid": 1}
]

# Отправка данных в Kafka
for purchase in purchases:
    producer.produce('purchases', json.dumps(purchase).encode('utf-8'))
    time.sleep(1)  # Задержка для имитации времени между покупками

for product in products:
    producer.produce('products', json.dumps(product).encode('utf-8'))

producer.flush()