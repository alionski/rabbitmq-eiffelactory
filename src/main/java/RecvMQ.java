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
    private String conumerTag = "lnxalenah";
    private volatile boolean alive = true;
    private boolean autoAck = false;
    private Thread receiverThread;

    /**
     * Called from the groovy script. Receives messages from the Eiffel exchange and
     * writes them to file.
     */
    public void startReceiving() {
        receiverThread = new Thread(() -> {
            try {
                Channel channel = rabbitConnection.newChannel();

                channel.basicConsume(RabbitConnection.QUEUE_NAME, autoAck, conumerTag,
                    new DefaultConsumer(channel) {
                        @Override
                        public void handleDelivery(String consumerTag,
                                                   Envelope envelope,
                                                   AMQP.BasicProperties props,
                                                   byte[] body) throws IOException {

                            String routingKey = envelope.getRoutingKey();
                            String contentType = props.getContentType();
                            long deliveryTag = envelope.getDeliveryTag();

                            receiveMessage(contentType, consumerTag, body);

                            channel.basicAck(deliveryTag, false);

                            if (!alive) {
                                shutdown(channel, consumerTag);
                            }
                        }
                    }
                );
            } catch (IOException e) {
                RabbitLogger.writeJavaError(e);
            }
        });
        receiverThread.start();
    }

    private void shutdown(Channel channel, String consumerTag) {
        try {
            channel.basicCancel(consumerTag);
            channel.queueUnbind(
                    RabbitConnection.QUEUE_NAME,
                    RabbitConnection.EXCHANGE_NAME,
                    RabbitConnection.ROUTING_KEY);
            channel.close();
            RabbitLogger.writeRabbitLog("Receiver closing channel...");
            rabbitConnection.closeConnection();
            RabbitLogger.writeRabbitLog("Receiver closing connection...");
            RabbitLogger.closeWriters(); // TODO: move from here later
        } catch (TimeoutException | IOException e) {
            RabbitLogger.writeJavaError(e);
        }
    }

    /**
     * Writes the Eiffel message to file
     * @param consumerTag
     * @param delivery
     */
    private void receiveMessage(String contentType, String consumerTag, byte[] delivery) {
        String message = new String(delivery, StandardCharsets.UTF_8);
        String log = "time : " + new Timestamp(new Date().getTime()) + "\n" +
                "consumer tag: " + consumerTag + "\n" +
                "content type : " + contentType + "\n" +
                "message : " +  message + "\n";
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