const express = require("express");
const { MongoClient } = require("mongodb");
const amqp = require("amqplib/callback_api");
const util = require('util');
const cors = require('cors');
const swaggerJsDoc = require("swagger-jsdoc");
const swaggerUi = require("swagger-ui-express");

const app = express();
app.use(express.json());
app.use(cors());

const config = {
    uri: "mongodb+srv://nikkljucevsek:OldbtLLbshDbB69v@cluster0.9uuzozi.mongodb.net/",
    dbName: "logsDB",
    collectionName: "logs",
    rabbitUser: "student",
    rabbitPassword: "student123",
    rabbitHost: "studentdocker.informatika.uni-mb.si",
    rabbitPort: "5672",
    vhost: "",
    exchange: 'upp-3',
    queue: 'upp-3',
    routingKey: 'zelovarnikey',
};

config.amqpUrl = util.format("amqp://%s:%s@%s:%s/%s", config.rabbitUser, config.rabbitPassword, config.rabbitHost, config.rabbitPort, config.vhost);

let db, collection;

// Swagger configuration
const swaggerOptions = {
    definition: {
        openapi: '3.0.0',
        info: {
            title: 'Log Service API',
            version: '1.0.0',
            description: 'A log service API',
        },
        servers: [
            {
                url: 'http://localhost:3001'
            }
        ]
    },
    // Path to the API docs
    apis: ['app.js'],
};

const swaggerDocs = swaggerJsDoc(swaggerOptions);
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocs));

MongoClient.connect(config.uri, { useUnifiedTopology: true })
    .then(client => {
        db = client.db(config.dbName);
        collection = db.collection(config.collectionName);

        setupRoutes();

        app.listen(3001, () => {
            console.log("Server is running on port 3001");
        });
    })
    .catch(error => {
        console.error("Failed to connect to MongoDB:", error);
        process.exit(1);
    });

function setupRoutes() {
    /**
     * @swagger
     * /logs:
     *   post:
     *     summary: Transfer logs
     *     responses:
     *       200:
     *         description: Logs transferred successfully
     */
    app.get("/logs/:dateFrom/:dateTo", getLogsHandler);
    app.post("/logs", postLogsHandler(config));
    

    /**
     * @swagger
     * /logs/{dateFrom}/{dateTo}:
     *   get:
     *     summary: Retrieve logs between two dates
     *     parameters:
     *       - in: path
     *         name: dateFrom
     *         required: true
     *         schema:
     *           type: string
     *       - in: path
     *         name: dateTo
     *         required: true
     *         schema:
     *           type: string
     *     responses:
     *       200:
     *         description: A list of logs
     *         content:
     *           application/json:
     *             schema:
     *               type: array
     *               items:
     *                 type: object
     *                 properties:
     *                   log:
     *                     type: string
     *                   timestamp:
     *                     type: string
     *                     format: date-time
     */
    app.get("/logs/:dateFrom/:dateTo", getLogsHandler);

    /**
     * @swagger
     * /logs:
     *   delete:
     *     summary: Delete all logs
     *     responses:
     *       200:
     *         description: Logs deleted successfully
     */
    app.delete("/logs", deleteLogsHandler);
}

function postLogsHandler({ amqpUrl, queue }) {
    return (req, res) => {
        amqp.connect(amqpUrl, (error, connection) => {
            if (error) {
                console.error("Error connecting to RabbitMQ:", error);
                return res.status(500).json({ error: "Failed to connect to RabbitMQ" });
            }

            connection.createChannel((error, channel) => {
                if (error) {
                    console.error("Error creating RabbitMQ channel:", error);
                    return res.status(500).json({ error: "Failed to create RabbitMQ channel" });
                }

                channel.assertQueue(queue, { durable: true });

                channel.consume(queue, (msg) => {
                    if (msg && msg.content) {
                        const log = msg.content.toString();

                        // Log the message being consumed for troubleshooting
                        console.log(`Received log from RabbitMQ: ${log}`);

                        collection.insertOne({ log, timestamp: new Date() })
                            .then(() => {
                                console.log(`Inserted log into MongoDB: ${log}`);
                                channel.ack(msg);
                            })
                            .catch(dbError => {
                                console.error("Error inserting log into MongoDB:", dbError);
                                channel.nack(msg);  // Negative acknowledge in case of an error
                            });
                    }
                }, { noAck: false });

                // Return response after setting up consumer
                res.status(200).json({ message: "Logs transfer in progress" });
            });
        });
    };
}



function getLogsHandler(req, res) {
    const dateFrom = new Date(req.params.dateFrom);
    const dateTo = new Date(req.params.dateTo);

    // Ensure the dates are valid
    if (isNaN(dateFrom.getTime()) || isNaN(dateTo.getTime())) {
        return res.status(400).json({ error: "Invalid date parameters" });
    }

    collection.find({
        timestamp: {
            $gte: dateFrom,
            $lt: dateTo
        }
    }).toArray().then(logs => {
        res.status(200).json(logs);
    }).catch(error => {
        console.error("Error retrieving logs:", error);
        res.status(500).json({ error: "Failed to retrieve logs" });
    });
}

function deleteLogsHandler(req, res) {
    collection.deleteMany({})
        .then(() => {
            res.status(200).json({ message: "Logs deleted successfully" });
        })
        .catch(error => {
            console.error("Error deleting logs:", error);
            res.status(500).json({ error: "Failed to delete logs" });
        });
}

process.on("SIGINT", () => {
    console.log("Shutting down server...");
    process.exit();
});
