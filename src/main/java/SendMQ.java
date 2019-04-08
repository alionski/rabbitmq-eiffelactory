import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Class representing a RabbitMQ client that sends Eiffel messages to the message bus.
 * Instantiated and used in the groovy script.
 */
public class SendMQ {
    private RabbitConnection rabbitConnection = RabbitConnection.getInstance();
    private Channel channel;

    /**
     * Called from the groovy script from a while loop in a thread to send messages
     * @param message -- Eiffel message to send
     */
	public void send(String message) {
        try {
            channel = rabbitConnection.getChannel();
            channel.basicPublish(RabbitConnection.EXCHANGE_NAME,
                                RabbitConnection.ROUTING_KEY,
                                new AMQP.BasicProperties.Builder()
                                        .contentType("application/json")
                                        .deliveryMode(2) // persistent
                                        .priority(1) // highest?
                                        .build(),
                                message.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            RabbitLogger.writeJavaError(e);
        }
	 }

    /**
     * Stops the sender, called from the groovy scipt as part of the executions{}. Workaround
     * for a bug in Artifactory where existing threads are not killed on reload.
     */
	 public void stopSending() {
         rabbitConnection.closeChannel(this);
     }
}
