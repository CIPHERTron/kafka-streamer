import kafka

try:
	print("Before consumer is made")
	consumer = kafka.KafkaConsumer(
		'product_orders',
		bootstrap_servers=['localhost:9092'],
		auto_offset_reset='earliest',
  enable_auto_commit=True,
		value_deserializer=lambda x: json.loads(x.decode('utf-8'))
	)
	print("After consumer is made")

	for message in consumer:
		if message is not None:
			print("Inside for loop")
			data = message.value.decode('utf-8')
			print("Received message is" + data)
		else:
			print("No entries")

	print("End of consumer block")

except Exception as e:
	print("Exception block")
	print(e)