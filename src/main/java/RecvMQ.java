import com.rabbitmq.client.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * Class representing a RabbitMQ client that listens to Eiffel messages.
 * Instantiated and used in the groovy script
 */
public class RecvMQ {
    private final static String QUEUE_NAME = RabbitConfig.getQueue();
    private final static String EXCHANGE_NAME = RabbitConfig.getExchange();
    private final static String EXCHANGE_TYPE = RabbitConfig.getExchangeType();
    private final static boolean QUEUE_DURABLE = true;
    private volatile boolean alive = true;
    private Thread thread;

    /**
     * Called from the groovy script. Receives messages from the Eiffel exchange and
     * writes them to file.
     */
    public void startReceiving() {
        thread = new Thread(() -> {
            try {
                Connection connection = initConnection();
                Channel channel = initChannel(connection);

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                    receiveMessage(consumerTag, delivery);
                    if (!alive) {
                        try {
                            channel.queueUnbind(QUEUE_NAME, EXCHANGE_NAME, "#");
                            channel.close();
                            connection.close();
                        } catch (TimeoutException e) {
                            RabbitLogger.writeJavaError(e);
                        }
                    }
                };
                channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });

            } catch (IOException | TimeoutException | KeyManagementException | NoSuchAlgorithmException e) {
                RabbitLogger.writeJavaError(e);
            }
        });
        thread.start();
    }

    /**
     * Writes the Eiffel message to file
     * @param consumerTag
     * @param delivery
     */
    private void receiveMessage(String consumerTag, Delivery delivery) {
        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        String res = new Timestamp(new Date().getTime()) + " : " + message + "\n";
        RabbitLogger.writeRabbitLog(res);
    }

    /**
     * Sets up a RabbitMQ connection
     * @return
     * @throws IOException
     * @throws TimeoutException
     */
    private Connection initConnection() throws IOException,
                                                TimeoutException,
                                                KeyManagementException,
                                                NoSuchAlgorithmException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(RabbitConfig.getUsername());
        factory.setPassword(RabbitConfig.getPassword());
        factory.setVirtualHost(RabbitConfig.getVhost());
        factory.setHost(RabbitConfig.getHostname());
        factory.setPort(RabbitConfig.getPort());
        factory.useSslProtocol();
        return factory.newConnection();
    }

    /**
     * Sets up a RabbitMQ channel
     * @param connection RabbitMQ connection
     * @return new Channel
     * @throws IOException
     */
    private Channel initChannel(Connection connection) throws IOException {
        Channel channel = connection.createChannel();
        declareExchange(channel);
        declareQueue(channel);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "#");
        return channel;
    }

    /**
     * Checks if the exchange exists, and if no creates a new one.
     * @param channel
     */
    private void declareExchange(Channel channel) {
        try {
            AMQP.Exchange.DeclareOk response = channel.exchangeDeclarePassive(EXCHANGE_NAME);
        } catch (IOException e) {
            RabbitLogger.writeJavaError(e);
            RabbitLogger.writeJavaError("Exchange " + EXCHANGE_NAME + " doesn't exist. Creating new");
            try {
                channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
            } catch (IOException e1) {
                RabbitLogger.writeJavaError(e1);
            }
        }
    }

    /**
     * Checks if the queue exists, and if no creates a new one.
     * @param channel
     */
    private void declareQueue(Channel channel) {
        try {
            AMQP.Queue.DeclareOk response = channel.queueDeclarePassive(QUEUE_NAME);
        } catch (IOException e) {
            RabbitLogger.writeJavaError(e);
            RabbitLogger.writeJavaError("Queue " + QUEUE_NAME + " doesn't exist. Creating new");
            try {
                channel.queueDeclare(QUEUE_NAME, QUEUE_DURABLE, false, false, null);
            } catch (IOException e1) {
                RabbitLogger.writeJavaError(e1);
            }
        }
    }

    /**
     * Stops the thread, called from the groovy scipt as part of the executions{}. Workaround
     * for a bug in Artifactory where existing threads are not killed on reload.
     */
    public void stopReceiving() {
        alive = false;
        thread.interrupt(); // doesn't work anyway
        RabbitLogger.closeWriters();
    }
}