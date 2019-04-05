import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

/**
 * Class representing a RabbitMQ client that sends Eiffel messages to the message bus.
 * Instantiated and used in the groovy script.
 */
public class SendMQ {
    // uncommented fields and method call are for real rabbitmq
    private final static String QUEUE_NAME = "alenah.eiffelactory.dev";
    private final static String EXCHANGE_NAME = "eiffel.public";
    private final static String EXCHANGE_TYPE = "topic";
    private Connection connection;
    private Channel channel;

    public SendMQ() {
        try {
            connection = initConnection();
        } catch (IOException | TimeoutException | KeyManagementException | NoSuchAlgorithmException e) {
            RabbitLogger.writeJavaError(e);
        }
    }

    /**
     * Called from the groovy script from a while loop in a thread to send messages
     * @param message -- Eiffel message to send
     */
	public void send(String message) {
        try {
            channel = connection.createChannel();
//            channel.exchangeDeclarePassive(EXCHANGE_NAME)
//            channel.queueDeclarePassive(QUEUE_NAME);
            channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "#");
            channel.basicPublish(EXCHANGE_NAME, "#", null, message.getBytes(StandardCharsets.UTF_8));
            channel.close();
        } catch (IOException | TimeoutException e) {
            RabbitLogger.writeJavaError(e);
        }
	 }

	 public void stop() {
         try {
             connection.close();
         } catch (IOException e) {
             RabbitLogger.writeJavaError(e);
         }
     }

    private Connection initConnection() throws IOException,
            TimeoutException,
            KeyManagementException,
            NoSuchAlgorithmException {
        RabbitConfig rabbitConfig = new RabbitConfig();
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(rabbitConfig.getUsername());
        factory.setPassword(rabbitConfig.getPassword());
        factory.setVirtualHost(rabbitConfig.getVhost());
        factory.setHost(rabbitConfig.getHostname());
        factory.setPort(rabbitConfig.getPort());
//        factory.useSslProtocol();
        return factory.newConnection();
    }
}
