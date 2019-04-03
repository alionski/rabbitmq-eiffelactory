import com.rabbitmq.client.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeoutException;

/**
 * Class representing a RabbitMQ client that listens to Eiffel messages.
 * Instantiated and used in the groovy script
 */
public class RecvMQ {

    private final static String QUEUE_NAME = "eiffelactory";
    private final static String EXCHANGE_NAME = "eiffel";
    private final static String EXCHANGE_TYPE = "fanout";
    private volatile boolean alive = true;
    private Logger logger = new Logger();

    /**
     * Called from the groovy script. Receives messages from the Eiffel exchange and
     * writes them to file.
     */
    public void startReceiving() {
        new Thread(() -> {
            try {
                Connection connection = initConnection();
                Channel channel = initChannel(connection);

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                    receiveMessage(consumerTag, delivery);
                    if (!alive) {
                        try {
                            channel.queueUnbind(QUEUE_NAME, EXCHANGE_NAME, "");
                            channel.close();
                            connection.close();
                        } catch (TimeoutException e) {
                            logger.writeJavaError(e.toString());
                        }
                    }
                };

                channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag -> { });

            } catch (IOException | TimeoutException e) {
                logger.writeJavaError(e.toString());
            }
        }).start();
    }

    /**
     * Writes the Eiffel message to file
     * @param consumerTag
     * @param delivery
     */
    private void receiveMessage(String consumerTag, Delivery delivery) {
        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        String res = new Timestamp(new Date().getTime()) + " : " + message + "\n";
        logger.writeRabbitLog(res);
    }

    /**
     * Sets up a RabbitMQ connection
     * @return
     * @throws IOException
     * @throws TimeoutException
     */
    private Connection initConnection() throws IOException, TimeoutException {
        RabbitConfig rabbitConfig = new RabbitConfig(logger);
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(rabbitConfig.getUsername());
        factory.setPassword(rabbitConfig.getPassword());
        factory.setVirtualHost(rabbitConfig.getVhost());
        factory.setHost(rabbitConfig.getHostname());
        factory.setPort(rabbitConfig.getPort());
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
        channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
        channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "");
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        return channel;
    }

    /**
     * Stops the thread, called from the groovy scipt as part of the executions{}. Workaround
     * for a bug in Artifactory where existing threads are not killed on reload.
     */
    public void stopReceiving() {
        alive = false;
    }
}