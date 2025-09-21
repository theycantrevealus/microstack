const { Kafka } = require("kafkajs");
const fs = require("fs");
const path = require("path");

// adjust paths to match your mounted certs inside the Node.js container/host
const certsDir = "/home/takashitanaka/microstack/kafka/certificates";

const kafka = new Kafka({
  clientId: "test-client",
  brokers: ["localhost:19094", "localhost:29094", "localhost:39094"],
  ssl: {
    rejectUnauthorized: false,
    ca: [
      fs.readFileSync(path.join(certsDir, "pem/kafka-cluster-ca.pem"), "utf-8"),
    ],
    cert: fs.readFileSync(
      path.join(certsDir, "client/kafka-client.pem"),
      "utf-8"
    ),
    key: fs.readFileSync(
      path.join(certsDir, "client/kafka-client.key.pem"),
      "utf-8"
    ),
  },
  sasl: {
    mechanism: "scram-sha-512",
    username: "kafkabroker",
    password: "confluent",
  },
  enforceRequestTimeout: true,
});

async function run() {
  const producer = kafka.producer();
  const consumer = kafka.consumer({ groupId: "test-group" });

  try {
    console.log("🔌 Connecting...");
    await producer.connect();
    await consumer.connect();

    console.log("📤 Sending test message...");
    await producer.send({
      topic: "test-topic",
      messages: [{ key: "key1", value: "Hello from KafkaJS over SASL_SSL!" }],
    });

    await consumer.subscribe({ topic: "test-topic", fromBeginning: true });

    consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log(
          `📥 Received: ${message.key?.toString()} => ${message.value?.toString()}`
        );
      },
    });
  } catch (err) {
    console.error("❌ Error:", err);
  }
}

run();
