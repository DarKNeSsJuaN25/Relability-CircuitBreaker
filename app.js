const express = require('express');
const axios = require('axios');
const { Client } = require('pg');
const amqp = require('amqplib');
const app = express();
const PORT = 8000;

const APIURL = "https://api.jikan.moe/v4/anime/";

const CircuitBreakerStates = {
  OPENED: "OPENED",
  CLOSED: "CLOSED",
  HALF: "HALF"
}

const dbdata = {
  user: "postgres",
  database: "postgres",
  host: "127.0.0.1",
  password: "Softjuandius_25",
  port: 5432
}

const client = new Client(dbdata);
client.connect();

let requestCounter = 0;
let errorCounter = 0;
let circuitState = CircuitBreakerStates.CLOSED;

const openCircuit = () => {
  circuitState = CircuitBreakerStates.OPENED;
  console.log('Circuit is OPEN now.');
}

const closeCircuit = () => {
  circuitState = CircuitBreakerStates.CLOSED;
  errorCounter = 0;
  console.log('Circuit is CLOSED now.');
}

const handleRetry = async (id) => {
  try {
    let isinDB = await client.query('SELECT title FROM animetwo where id= ($1)', [id]);
    let response;
    if (isinDB.rows.length === 0) {
      let apiResponse = await axios.get(APIURL + id);
      response = apiResponse.data.data.title;
      await client.query('INSERT INTO animetwo (id, title) VALUES ($1,$2)', [id, response]);
    } else {
      response = isinDB.rows[0].title;
    }
    console.log(response);
  } catch (err) {
    console.log(err.message);
    errorCounter++;
    if (errorCounter > 400) {
      openCircuit();
      return;
    }
    setTimeout(() => handleRetry(id), 1000); // reintentar después de 1 segundo
  }
};

async function getAnime(req, res) {
  try {
    requestCounter++;
    if (circuitState === CircuitBreakerStates.OPENED) {
      res.status(500).send('Service Unavailable');
      return;
    }
    const id = req.query.id;
    let isinDB = await client.query('SELECT title FROM animetwo where id= ($1)',[id]);
    // console.log(isinDB.rows[0].title);
    let response;
    if(isinDB.rows.length === 0){
        response = await axios.get(APIURL + id);
        response = response.data.data.title;
        await client.query('INSERT INTO animetwo (id, title) VALUES ($1,$2)', [id,response]);
        // console.log("Insertando en DB");
    }
    else response = isinDB.rows[0].title;
    console.log(response);
    res.send(response);
  } catch (err) {
    console.log(err.message);
    errorCounter++;
    if (errorCounter > 400) {
      openCircuit();
      return;
    }
    // Push the failed request to RabbitMQ for retry
    const queueName = 'retry_queue';
    const connection = await amqp.connect('amqp://localhost');
    const channel = await connection.createChannel();
    await channel.assertQueue(queueName, { durable: true });
    channel.sendToQueue(queueName, Buffer.from(JSON.stringify({ query: req.query })), { persistent: true });
  } finally {
    res.end();
  }
}

app.get('/getanime', getAnime);

app.listen(PORT, (req, res) => {
  console.log(`Escuchando puerto ${PORT}`);
});
