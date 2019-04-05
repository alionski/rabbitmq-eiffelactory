import com.rabbitmq.client.*;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;

public class RabbitConnection {
    final static String QUEUE_NAME = RabbitConfig.getQueue();
    final static String EXCHANGE_NAME = RabbitConfig.getExchange();
    final static String EXCHANGE_TYPE = RabbitConfig.getExchangeType();
    final static String ROUTING_KEY = RabbitConfig.getRoutingKey();
    final static boolean QUEUE_DURABLE = true;
    private Connection connection;
    private Channel channel;

    private Connection initConnection() throws IOException,
            TimeoutException,
            KeyManagementException,
            NoSuchAlgorithmException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(RabbitConfig.getUsername());
        factory.setPassword(RabbitConfig.getPassword());
        factory.setVirtualHost(RabbitConfig.getVhost());
        factory.setHost(RabbitConfig.getHostname());
        factory.setPort(RabbitConfig.getPort());
        factory.useSslProtocol();

        Connection connection = factory.newConnection();
        connection.addShutdownListener(new RabbitShutdownListener());
        return connection;
    }

    // TODO: "Consuming in one thread and publishing in another thread on a shared channel can be safe." --> share channel?
    public Channel newChannel() {
        try {
            connection = initConnection();
            createChannel();
            declareExchange();
            declareQueue();
        } catch (IOException | TimeoutException | KeyManagementException | NoSuchAlgorithmException e) {
            RabbitLogger.writeJavaError(e);
        }
        return channel;
    }

    public void createChannel() throws IOException {
        channel = connection.createChannel();
    }

    private void declareExchange() {
        try {
            AMQP.Exchange.DeclareOk response = channel.exchangeDeclarePassive(EXCHANGE_NAME);
        } catch (IOException e) {
            RabbitLogger.writeJavaError(e);
            RabbitLogger.writeJavaError("Exchange " + EXCHANGE_NAME + " doesn't exist. Creating new");
            try {
                createChannel();
                channel.exchangeDeclare(EXCHANGE_NAME, EXCHANGE_TYPE);
            } catch (IOException e1) {
                RabbitLogger.writeJavaError(e1);
            }
        }
    }

    private void declareQueue() {
        try {
            AMQP.Queue.DeclareOk response = channel.queueDeclarePassive(QUEUE_NAME);
        } catch (IOException e) {
            RabbitLogger.writeJavaError(e);
            RabbitLogger.writeJavaError("Queue " + QUEUE_NAME + " doesn't exist. Creating new");
            try {
                createChannel();
                channel.queueDeclare(QUEUE_NAME, QUEUE_DURABLE, false, false, null);
            } catch (IOException e1) {
                RabbitLogger.writeJavaError(e1);
            }
        } finally {
            try {
                channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
            } catch (IOException e) {
                RabbitLogger.writeJavaError(e);
            }
        }
    }


    public void closeConnection() {
        try {
            connection.close();
        } catch (IOException e) {
            RabbitLogger.writeJavaError(e);
        }
    }

    private class RabbitShutdownListener implements ShutdownListener {

        @Override
        public void shutdownCompleted(ShutdownSignalException cause) {
            if (cause.isHardError()) { // it's a connection-level error
                Connection connection = (Connection) cause.getReference();
                if (!cause.isInitiatedByApplication()) {
                    Method reason = cause.getReason();
                    RabbitLogger.writeJavaError(reason.toString());
                    Throwable thrown = cause.getCause();
                }
            } else { // it's a channel-level error
                Channel channel = (Channel) cause.getReference();
                if (!cause.isInitiatedByApplication()) {
                    Method reason = cause.getReason();
                    RabbitLogger.writeJavaError(reason.toString());
                    Throwable thrown = cause.getCause();
                }

            }
        }
    }
}
