from flask import Flask, request, jsonify
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
import psycopg2
import json

app = Flask(__name__)

KAFKA_BOOTSTRAP_SERVERS='kafka'
KAFKA_TOPIC='product_orders'


# function to establish postgres connection
def create_connection():
    conn = psycopg2.connect(
        host='db',
        database='orders',
        user="pritishsamal",
        password="Cymatics@7",
    )
    return conn

# Dummy function to send email
def send_email(order):
    print(f"Email sent successfully to {order['customer']['email']}...")

# Adding success callback
def on_producer_send_success(record_metadata):
    print('Message sent successfully!')

# Adding failure callback
def on_producer_send_error(excp):
    print('Failed to send message:', excp)

def publish_to_kafka_topic(topic, message):
    producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks="all",
                retries=0,
                request_timeout_ms=2000,
                batch_size=16384,
                linger_ms=100
            )
    future = producer.send(topic, value=message)
    producer.flush()
    future.add_callback(on_producer_send_success)
    future.add_errback(on_producer_send_error)
    producer.close()

def consume_from_kafka_topic(topic):
    consumer = KafkaConsumer(topic, bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, group_id='kafka-streamer')
    return consumer

def save_order_to_postgres(order):
    conn = create_connection()
    cursor = conn.cursor()

    query = """
        INSERT INTO orders (order_id, name, email, street, city, state, postal_code, product_name, quantity, order_date, priority)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    values = (
        order.get('order_id'),
        order.get('customer').get('name'),
        order.get('customer').get('email'),
        order.get('customer').get('address').get('street'),
        order.get('customer').get('address').get('city'),
        order.get('customer').get('address').get('state'),
        order.get('customer').get('address').get('postal_code'),
        order.get('product_name'),
        order.get('quantity'),
        order.get('order_date'),
        order.get('priority')
    )

    cursor.execute(query, values)
    conn.commit()

    cursor.close()
    conn.close()

def consume_and_send_emails():
    consumer = consume_from_kafka_topic(KAFKA_TOPIC)

    for message in consumer:
        # print("Message", message.value)
        order = eval(message.value.decode('utf-8'))
        # print("Order", order.get('order_id'))

        # Check if priority is high
        if order.get('priority') == 'high':
            send_email(order)
        
        # Save order to PostgreSQL
        save_order_to_postgres(order)

        # Write message to respective city topic
        city_topic = order.get('customer').get('address').get('city')
        publish_to_kafka_topic(city_topic, order)


# Dummy Home Endpoint
@app.route("/")
def hello():
    return "Hey there, welcome to the Kafka streamer!"

# CREATE and READ Orders Endpoint
@app.route("/orders", methods=["POST", "GET"])
def orders():
    if request.method == "GET":
        conn = create_connection()
        cursor = conn.cursor()

        query = "SELECT * FROM orders;"
        cursor.execute(query)
        rows = cursor.fetchall()

        orders = []
        for row in rows:
            order = {
                'order_id': row[0],
                'name': row[1],
                'email': row[2],
                'street': row[3],
                'city': row[4],
                'state': row[5],
                'postal_code': row[6],
                'product_name': row[7],
                'quantity': row[8],
                'order_date': row[9].strftime('%Y-%m-%d'),
                'priority': row[10]
            }
            orders.append(order)

        cursor.close()
        conn.close()

        return jsonify(orders), 200
    
    if request.method == "POST":
        order = request.get_json()
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks="all",
            retries=0,
            request_timeout_ms=2000,
            batch_size=16384,
            linger_ms=100
        )
        future = producer.send(KAFKA_TOPIC, value=order)
        producer.flush()
        future.add_callback(on_producer_send_success)
        future.add_errback(on_producer_send_error)
        producer.close()

        return jsonify({'message': 'Order created successfully'}), 200


if __name__ == '__main__':
    # Start consuming and sending emails in the background
    import threading
    email_thread = threading.Thread(target=consume_and_send_emails)
    email_thread.start()

    # Start the Flask web service
    app.run(debug=True)