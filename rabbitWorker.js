const amqp = require('amqplib');
const axios = require('axios');
const { Client } = require('pg');
const rateLimit = require("express-rate-limit");

const API_URL = "https://api.jikan.moe/v4/anime/";

const dbConfig = {
    user: "postgres",
    database: "postgres",
    host: "127.0.0.1",
    password: "1234",
    port: 5432
}

const client = new Client(dbConfig);
client.connect();

const handleRetry = async (id) => {
    try {
        let response = await axios.get(API_URL + id);
        if (!response.ok) {
            throw new Error('Failed to fetch data');
        }

        // process the data
        response = data.data.data.title;
        console.log('res: ', response);
        await client.query('INSERT INTO animetwo (id, title) VALUES ($1,$2)', [id, response]);

    } catch (err) {
        console.log('Error handleRetry: ', err.message);
        // retry the query after a delay
        await new Promise(resolve => setTimeout(resolve, 5000));
        await handleRetry(id);
    }
};

const startWorker = async () => {
  const connection = await amqp.connect('amqp://localhost');
  const channel = await connection.createChannel();
  const queueName = 'retry_queue';

  console.log('Worker is waiting for messages');
  await channel.assertQueue(queueName, { durable: true });
  channel.consume(queueName, async (msg) => {
    const { id } = JSON.parse(msg.content.toString());
    try {
      // apply rate limiting
      const limiter = rateLimit({
        windowMs: 1000, // 1 second
        max: 3, // limit each IP to 3 requests per windowMs
      });
      await limiter(null, null, async () => {
        await handleRetry(id);
      });
      channel.ack(msg);
    } catch (err) {
      console.log(err.message);
      channel.nack(msg);
    }
  });
};

startWorker();