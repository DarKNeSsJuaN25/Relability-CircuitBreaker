const express = require('express');
const axios = require('axios');
const { Client } = require('pg');
const amqp = require('amqplib');
const rateLimit = require("express-rate-limit");
const app = express();
const PORT = 8000;

const API_URL = "https://api.jikan.moe/v4/anime/";

const CircuitBreakerStates = {
	OPENED: "OPENED",
	CLOSED: "CLOSED",
	HALF: "HALF"
}

const dbConfig = {
	user: "postgres",
	database: "postgres",
	host: "127.0.0.1",
	password: "1234",
	port: 5432
}

const client = new Client(dbConfig);
client.connect();

const MAX_ERRORS = 4;
let requestCounter = 0;
let errorCounter = 0;
let circuitState = CircuitBreakerStates.CLOSED;

// Open the circuit breaker
const openCircuit = () => {
	circuitState = CircuitBreakerStates.OPENED;
	console.log('Circuit is OPEN now.');
}

// Close the circuit breaker
const closeCircuit = () => {
	circuitState = CircuitBreakerStates.CLOSED;
	errorCounter = 0;
	console.log('Circuit is CLOSED now.');
}

// Get anime by id
const getAnimeById = async (id) => {
	try {
		let isinDB = await client.query('SELECT title FROM animetwo where id= ($1)', [id]);
		let response;
		console.log('db response: ', isinDB.rows);
		if (isinDB.rows.length === 0) {
			response = await axios.get(API_URL + id, { timeout: 1000 }); // set max timeout to 10 seconds
			if (response.status === 429) {
				throw new Error('Rate limit exceeded');
			}
			response = response.data.data.title;
			await client.query('INSERT INTO animetwo (id, title) VALUES ($1,$2)', [id, response]);
		}
		else response = isinDB.rows[0].title;
		return response;
	} catch (err) {
		console.log(err.message);
		throw err;
	}
}

// Handle the request to get anime by id
const handleGetAnime = async (req, res) => {
	try {
		requestCounter++;
		if (circuitState === CircuitBreakerStates.OPENED) {
			res.status(500).send('Service Unavailable');
			return;
		}
		const id = req.query.id;
		console.log(`Request #${requestCounter} to get anime with id ${id}`);
		const response = await getAnimeById(id);
		if (!response) {
			res.status(404).send('Anime not found');
			return;
		}
		res.send(response);
	} catch (err) {
		console.log(err.message);
		errorCounter++;
		if (errorCounter > MAX_ERRORS) {
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

// Set up rate limiter
const limiter = rateLimit({
	windowMs: 1000, // 1 second
	max: 3, // limit each IP to 3 requests per windowMs
	message: "Too many requests from this IP, please try again later"
});

// Apply rate limiter to all requests
app.use(limiter);

// Set up the app
app.get('/getanime', handleGetAnime);

app.listen(PORT, () => {
	console.log(`Listening on port ${PORT}`);
});
