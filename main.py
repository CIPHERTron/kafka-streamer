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


if __name__ == "__main__":
    app.run(debug=True)
