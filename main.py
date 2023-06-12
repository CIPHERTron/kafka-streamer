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

# Create Topic if not present
def create_topic_if_not_exists(topic):
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)

    topic_metadata = admin_client.list_topics()

    if topic not in topic_metadata.topics:
        topic = NewTopic(
            name=topic,
            num_partitions=3,
            replication_factor=1
        )

        admin_client.create_topics([topic])

def publish_to_kafka_topic(topic, message):
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    producer.send(topic, value=message.encode('utf-8'))
    producer.flush()
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
        order['order_id'],
        order['customer']['name'],
        order['customer']['email'],
        order['customer']['address']['street'],
        order['customer']['address']['city'],
        order['customer']['address']['state'],
        order['customer']['address']['postal_code'],
        order['product_name'],
        order['quantity'],
        order['order_date'],
        order['priority']
    )

    cursor.execute(query, values)
    conn.commit()

    cursor.close()
    conn.close()

def consume_and_send_emails():
    consumer = consume_from_kafka_topic(KAFKA_TOPIC)

    for message in consumer:
        print(json.loads(message.value))
        order = eval(message.value.decode('utf-8'))

        # # Check if priority is high
        if order['priority'] == 'high':
            send_email(order)
        # Save order to PostgreSQL
        save_order_to_postgres(order)

        # # Write message to respective city topic
        create_topic_if_not_exists(order['customer']['address']['city'])
        city_topic = order['customer']['address']['city']
        publish_to_kafka_topic(city_topic, message.value.decode('utf-8'))


# Dummy Home Endpoint
@app.route("/")
def hello():
    return "Hey there, welcome to the Kafka streamer!"

@app.route("/kafka-data", methods=["GET"])
def get_data():
    try:
        consumer = consume_from_kafka_topic(KAFKA_TOPIC)
        print(consumer)
        
        # print(type(consumer()))
        # orders = []
        # for message in consumer:
            # order = eval(message.value.decode('utf-8'))
            # print(json.loads(message.value))
        return "/kafka-data endpoint hit"
    except Exception as e:
        print(e)
        return "Some shit went wrong with kafka consumer"

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

        # Publish order to Kafka topic
        publish_to_kafka_topic(KAFKA_TOPIC, str(order))

        return jsonify({'message': 'Order created successfully'}), 200


if __name__ == '__main__':
    # Start consuming and sending emails in the background
    import threading
    email_thread = threading.Thread(target=consume_and_send_emails)
    email_thread.start()

    # Start the Flask web service
    app.run(debug=True)