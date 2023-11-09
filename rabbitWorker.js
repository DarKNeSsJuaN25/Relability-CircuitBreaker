const amqp = require('amqplib');

const handleRetry = async (id) => {
  // Your retry logic here
};

const startWorker = async () => {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();
  const queueName = 'retry_queue';

  await channel.assertQueue(queueName, { durable: true });
  channel.consume(queueName, async (msg) => {
    const { id } = JSON.parse(msg.content.toString());
    try {
      await handleRetry(id);
      channel.ack(msg);
    } catch (err) {
      console.log(err.message);
      channel.nack(msg);
    }
  });
};

startWorker();