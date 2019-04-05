import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

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
        return factory.newConnection();
    }

    public Channel newChannel() {
        try {
            connection = initConnection();
            channel = connection.createChannel();
            declareExchange();
            declareQueue();
            channel.queueBind(QUEUE_NAME, EXCHANGE_NAME, ROUTING_KEY);
        } catch (IOException | TimeoutException | KeyManagementException | NoSuchAlgorithmException e) {
            RabbitLogger.writeJavaError(e);
        }
        return channel;
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


    public void closeConnection() {
        try {
            connection.close();
        } catch (IOException e) {
            RabbitLogger.writeJavaError(e);
        }
    }

}
