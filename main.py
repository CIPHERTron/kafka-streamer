from flask import Flask, request

app = Flask(__name__)


@app.route("/")
def hello():
    return "Hey there, welcome to the Kafka streamer!"


@app.route("/orders", methods=["POST", "GET"])
def orders():
    if request.method == "GET":
        return "GET route hit"
    else:
        return "POST route hit."


if __name__ == "__main__":
    app.run(debug=True)
