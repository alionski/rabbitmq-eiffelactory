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
    private RabbitConnection rabbitConnection = new RabbitConnection();
    private volatile boolean alive = true;
    private Thread receiverThread;

    /**
     * Called from the groovy script. Receives messages from the Eiffel exchange and
     * writes them to file.
     */
    public void startReceiving() {
        receiverThread = new Thread(() -> {
            try {
                Channel channel = rabbitConnection.newChannel();

                DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                    receiveMessage(consumerTag, delivery);

                    if (!alive) {
                        try {
                            channel.queueUnbind(
                                    RabbitConnection.QUEUE_NAME,
                                    RabbitConnection.EXCHANGE_NAME,
                                    RabbitConnection.ROUTING_KEY);
                            channel.close();
                            RabbitLogger.writeRabbitLog("Receiver closing channel...");
                            rabbitConnection.closeConnection();
                            RabbitLogger.writeRabbitLog("Receiver closing connection...");
                            RabbitLogger.closeWriters(); // TODO: move from here later
                        } catch (TimeoutException e) {
                            RabbitLogger.writeJavaError(e);
                        }
                    }
                };
                channel.basicConsume(RabbitConnection.QUEUE_NAME, true, deliverCallback, consumerTag -> { });

            } catch (IOException e) {
                RabbitLogger.writeJavaError(e);
            }
        });
        receiverThread.start();
    }

    /**
     * Writes the Eiffel message to file
     * @param consumerTag
     * @param delivery
     */
    private void receiveMessage(String consumerTag, Delivery delivery) {
        String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
        String log = new Timestamp(new Date().getTime()) + " : " + message + "\n";
        RabbitLogger.writeRabbitLog(log);
    }

    /**
     * Stops the receiverThread, called from the groovy scipt as part of the executions{}. Workaround
     * for a bug in Artifactory where existing threads are not killed on reload.
     */
    public void stopReceiving() {
        alive = false;
        receiverThread.interrupt(); // doesn't work anyway
    }
}