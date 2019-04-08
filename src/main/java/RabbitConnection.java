import com.rabbitmq.client.*;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeoutException;


/**
 * Singleton lass representing a RabbitMQ connection. Creates one shared connection and channel that will be used by both the
 * sender and the consumer.
 */
public class RabbitConnection {
    private static RabbitConnection rabbit = new RabbitConnection(); //not final because we might need to recreate it
    final static String QUEUE_NAME = RabbitConfig.getQueue();
    final static String EXCHANGE_NAME = RabbitConfig.getExchange();
    final static String EXCHANGE_TYPE = RabbitConfig.getExchangeType();
    final static String ROUTING_KEY = RabbitConfig.getRoutingKey();
    final static boolean QUEUE_DURABLE = true;
    private Connection connection;
    private Channel channel;

    private boolean senderClosed = false;
    private boolean receiverClosed = false;

    private RabbitConnection() { }

    public static RabbitConnection getInstance() {
        return rabbit;
    }

    /**
     *Creates a new connection
     * @return
     * @throws IOException
     * @throws TimeoutException
     * @throws KeyManagementException
     * @throws NoSuchAlgorithmException
     */
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

    /**
     * Creates new connection and channel
     * @return
     */
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

    /**
     * Creates a new channel from the given exception
     * @throws IOException
     */
    public void createChannel() throws IOException {
        channel = connection.createChannel();
    }

    /**
     * Checks if there is the specified exchange, and if no creates it
     */
    private void declareExchange() {
        try {
            // checking if the exchange exists
            AMQP.Exchange.DeclareOk response = channel.exchangeDeclarePassive(EXCHANGE_NAME);
        } catch (IOException e) {
            // there is no such exchange; because of the exception the channel close and we need to create a new
            // one and declare the exchange
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

    /**
     * Checks if there is the specified queue, and if no creates and binds it
     */
    private void declareQueue() {
        try {
            // checking if the channel exists
            AMQP.Queue.DeclareOk response = channel.queueDeclarePassive(QUEUE_NAME);
        } catch (IOException e) {
            // there is no such queue; because of the exception the channel close and we need to create a new
            // one and declare the queue
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

    /**
     * Returns the existing channel, and if none exists creates a new one.
     * @return
     */
    public Channel getChannel() {
        if (channel == null || !channel.isOpen()) {
            return newChannel();
        } else {
            return channel;
        }
    }

    /**
     * Shutdown listener called when the connection or channel closes. Logs errors cause by the server.
     */
    private class RabbitShutdownListener implements ShutdownListener {

        @Override
        public void shutdownCompleted(ShutdownSignalException cause) {
            if (cause.isHardError()) { // it's a connection-level error
                Connection connection = (Connection) cause.getReference();
                if (!cause.isInitiatedByApplication()) { // log only if connection closed by server
                    Method reason = cause.getReason();
                    RabbitLogger.writeShutdownError(reason.toString());
                    Throwable thrown = cause.getCause();
                    RabbitLogger.writeShutdownError(thrown);
                    RabbitLogger.closeShutdownWriter();
                }
            } else if (!cause.isHardError()) { // it's a channel-level error
                if (!cause.isInitiatedByApplication()) { // log only if channel closed by server
                    Method reason = cause.getReason();
                    RabbitLogger.writeShutdownError(reason.toString());
                    Throwable thrown = cause.getCause();
                    RabbitLogger.writeShutdownError(thrown);
                    RabbitLogger.closeShutdownWriter();
                }
            }
        }
    }

    /**
     * Closes the channel and connection if both the sender and consumer died, otherwise changes liveness status
     * @param obj
     */
    public void closeChannel(Object obj) {
        if (obj instanceof RecvMQ) {
            RabbitLogger.writeRabbitLog("Receiver closing channel...");
            receiverClosed = true;
        } else if (obj instanceof SendMQ) {
            RabbitLogger.writeRabbitLog("Sender closing channel...");
            senderClosed = true;
        }

        if (receiverClosed && senderClosed) {
            try {
                channel.close();
                connection.close();
                RabbitLogger.writeRabbitLog("Closing connection...");
                RabbitLogger.closeWriters();
            } catch (IOException | TimeoutException e) {
                e.printStackTrace();
            }
        }
    }
}
