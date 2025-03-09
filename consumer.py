import json
import time
from confluent_kafka import Consumer
import pandas as pd
from datetime import datetime, timedelta

# Настройки Kafka
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(conf)
consumer.subscribe(['purchases', 'products'])

# Хранение данных
purchase_data = []
product_data = []

def calculate_alert():
    global purchase_data, product_data
    now = datetime.now()
    one_minute_ago = now - timedelta(minutes=1)

    # Фильтрация покупок за последнюю минуту
    recent_purchases = [p for p in purchase_data if p['timestamp'] > one_minute_ago]
    
    # Объединение данных о покупках и продуктах
    df = pd.DataFrame(recent_purchases)
    df_products = pd.DataFrame(product_data)

    # Отладочные сообщения
    print("Recent Purchases DataFrame:")
    print(df)
    print("Products DataFrame:")
    print(df_products)

    # Проверка наличия необходимых столбцов
    if 'id' not in df_products.columns:
        print("Error: 'id' column not found in products DataFrame.")
        return
    if 'productid' not in df.columns:
        print("Error: 'productid' column not found in purchases DataFrame.")
        return

    # Объединение по productid
    merged = df.merge(df_products, left_on='productid', right_on='id', suffixes=('_purchase', '_product'))
    merged['total'] = merged['quantity'] * merged['price']

    # Проверка условия для алерта
    if merged['total'].sum() > 3000:
        print("Alert: Total sales in the last minute exceeded 3000!")

while True:
    msg = consumer.poll(1.0)  # Ожидание сообщения
    if msg is None:
        continue
    if msg.error():
        print(f"Error: {msg.error()}")
        continue

    # Обработка сообщения
    data = json.loads(msg.value().decode('utf-8'))
    if msg.topic() == 'purchases':
        data['timestamp'] = datetime.now()  # Добавление временной метки
        purchase_data.append(data)
    elif msg.topic() == 'products':
        product_data.append(data)
        print(f"Received product: {data}")  # Отладочное сообщение

    calculate_alert()

consumer.close()