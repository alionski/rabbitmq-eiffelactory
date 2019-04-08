import com.rabbitmq.client.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.util.Date;

/**
 * Class representing a RabbitMQ client that listens to Eiffel messages.
 * Instantiated and used in the groovy script
 */
public class RecvMQ {
    private RabbitConnection rabbitConnection = RabbitConnection.getInstance();
    private String consumerTag = RabbitConfig.getConsumerTag();
    private volatile boolean alive = true;
    private boolean autoAck = false;
    private ScriptCallback scriptCallback;

    /**
     * Called from the groovy script. Receives messages from the Eiffel exchange and
     * writes them to file.
     */

    public RecvMQ(ScriptCallback scriptCallback) {
        this.scriptCallback = scriptCallback;
    }

    public void startReceiving() {
        try {
            Channel channel = rabbitConnection.getChannel();

            channel.basicConsume(RabbitConnection.QUEUE_NAME, autoAck, consumerTag,
                new DefaultConsumer(channel) {

                    @Override
                    public void handleDelivery(String consumerTag,
                                               Envelope envelope,
                                               AMQP.BasicProperties props,
                                               byte[] body) throws IOException {
                        // it will not receive or ack any messages that arrive during shutdown
                        if (alive) {
                            receiveMessage(props.getContentType(),
                                    consumerTag,
                                    body);

                            channel.basicAck(envelope.getDeliveryTag(),
                                    false);
                        }
                    }
                }
            );
        } catch (IOException e) {
            RabbitLogger.writeJavaError(e);
        }
    }

    /**
     * Cancels the consumer and unbinds the queue
     * @param channel
     * @param consumerTag
     */
    private void shutdown(Channel channel, String consumerTag) {
        try {
            RabbitLogger.writeRabbitLog("Cancelling consumer...");
            channel.basicCancel(consumerTag);
            channel.queueUnbind(
                    RabbitConnection.QUEUE_NAME,
                    RabbitConnection.EXCHANGE_NAME,
                    RabbitConnection.ROUTING_KEY);
        } catch (IOException e) {
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
        if (alive) {
            scriptCallback.deliverEiffelMessage(message);
            RabbitLogger.writeRabbitLog(log);
        }
    }

    /**
     * Stops the receiverThread, called from the groovy scipt as part of the executions{}. Workaround
     * for a bug in Artifactory where existing threads are not killed on reload.
     */
    public void stopReceiving() {
        alive = false;
        shutdown(rabbitConnection.getChannel(), consumerTag);
        rabbitConnection.closeChannel(this);
    }
}