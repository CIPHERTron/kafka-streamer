from flask import Flask, request
from os import environ
import psycopg2

app = Flask(__name__)

def create_connection():
    conn = psycopg2.connect(
        host='localhost',
        database='orders',
        user=environ.get("POSTGRES_USERNAME"),
        password=environ.get("POSTGRES_PASSWORD"),
    )
    return conn

@app.route("/")
def hello():
    return "Hey there, welcome to the Kafka streamer!"


@app.route("/orders", methods=["POST", "GET"])
def orders():
    if request.method == "GET":
        conn = create_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM orders;")
        orders = cursor.fetchall()
        return orders
    
    if request.method == "POST":
        data = request.get_json()
        order_id = data.get('order_id')
        customer_name = data.get('customer').get('name')
        customer_email = data.get('customer').get('email')
        customer_street = data.get('customer').get('address').get('street')
        customer_city = data.get('customer').get('address').get('city')
        customer_state = data.get('customer').get('address').get('state')
        customer_postal_code = data.get('customer').get('address').get('postal_code')
        product_name = data.get('product_name')
        quantity = data.get('quantity')
        order_date = data.get('order_date')
        priority = data.get('priority')

        conn = create_connection()
        cursor = conn.cursor()

        query = """
        INSERT INTO orders (order_id, customer_name, customer_email, customer_street, customer_city, 
                            customer_state, customer_postal_code, product_name, quantity, order_date, priority)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """

        cursor.execute(query, (order_id, customer_name, customer_email, customer_street, customer_city,
                            customer_state, customer_postal_code, product_name, quantity, order_date, priority))

        conn.commit()
        conn.close()
        return "Order added successfully!!"


if __name__ == "__main__":
    app.run(debug=True)
