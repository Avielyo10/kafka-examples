from confluent_kafka import Consumer, KafkaError


c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'mygroup',
    'auto.offset.reset': 'earliest'
})

c.subscribe(['my-topic'])
try:
    while True:
        msg = c.poll(1.0)

        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        print(f"Received message: {msg.value().decode('utf-8')}")

except KeyboardInterrupt as e:
    print("Aborted by user")

finally:
    c.close()
