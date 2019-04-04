import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;

/**
 * Class representing a RabbitMQ client that sends Eiffel messages to the message bus.
 * Instantiated and used in the groovy script.
 */
public class SendMQ {

    private final static String QUEUE_NAME = "eiffel.public";
    private final static String EXCHANGE_NAME = "alenah.eiffelactory.dev";
    private final static String EXCHANGE_TYPE = "topic";
    private Logger rabbitLogger = new Logger();

    /**
     * Called from the groovy script from a while loop in a thread to send messages
     * @param message -- Eiffel message to send
     */
	public void send(String message) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost( new RabbitConfig(rabbitLogger).getHostname() );
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes(StandardCharsets.UTF_8));
		 } catch (Exception e) {
            rabbitLogger.writeJavaError(e);
        }
	 }
}
