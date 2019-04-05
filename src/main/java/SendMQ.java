import com.rabbitmq.client.AMQP;
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
    private final static String QUEUE_NAME = "alenah.eiffelactory.dev";
    private final static String EXCHANGE_NAME = "eiffel.public";
    private final static String EXCHANGE_TYPE = "topic";
    private final static boolean QUEUE_DURABLE = true;
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
            declareExchange();
            declareQueue();
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, "#");
            channel.basicPublish(EXCHANGE_NAME, "#", null, message.getBytes(StandardCharsets.UTF_8));
            channel.close();
        } catch (IOException | TimeoutException e) {
            RabbitLogger.writeJavaError(e);
        }
	 }

    private void declareQueue() {
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

    private void declareExchange() {
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
        factory.useSslProtocol();
        return factory.newConnection();
    }
}
