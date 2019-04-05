import com.rabbitmq.client.AMQP;
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
    private Channel channel;

    public SendMQ() {
        channel = rabbitConnection.newChannel();
    }

    /**
     * Called from the groovy script from a while loop in a thread to send messages
     * @param message -- Eiffel message to send
     */
	public void send(String message) {
        try {
            if (channel == null || !channel.isOpen()) {
                channel = rabbitConnection.newChannel();
            }
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

	 public void stop() {
         try {
             channel.close();
             RabbitLogger.writeRabbitLog("Sender closing channel...");
         } catch (IOException | TimeoutException e) {
             RabbitLogger.writeJavaError(e);
         }
         rabbitConnection.closeConnection();
         RabbitLogger.writeRabbitLog("Sender closing connection...");
     }
}
