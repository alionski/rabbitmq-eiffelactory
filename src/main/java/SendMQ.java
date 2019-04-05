import com.rabbitmq.client.Channel;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * Class representing a RabbitMQ client that sends Eiffel messages to the message bus.
 * Instantiated and used in the groovy script.
 */
public class SendMQ {
    private RabbitConnection rabbitConnection = new RabbitConnection();

    /**
     * Called from the groovy script from a while loop in a thread to send messages
     * @param message -- Eiffel message to send
     */
	public void send(String message) {
        try {
            Channel channel = rabbitConnection.newChannel();
            channel.basicPublish(RabbitConnection.EXCHANGE_NAME,
                                RabbitConnection.ROUTING_KEY,
                            null,
                                message.getBytes(StandardCharsets.UTF_8));
            channel.close();
        } catch (IOException | TimeoutException e) {
            RabbitLogger.writeJavaError(e);
        }
	 }

	 public void stop() {
         rabbitConnection.closeConnection();
     }


}
