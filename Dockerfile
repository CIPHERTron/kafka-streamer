FROM python:3.9

WORKDIR /server

COPY requirements.txt /server

RUN pip install --no-cache-dir -r requirements.txt

COPY . /server

ENV FLASK_APP=main.py

EXPOSE 5000

CMD ["flask", "run", "--host=0.0.0.0"]
