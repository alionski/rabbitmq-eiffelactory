import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.FileWriter;
import java.sql.Timestamp;
import java.util.Date;
import java.util.concurrent.TimeoutException;

public class RecvMQ {

    private final static String QUEUE_NAME = "eiffelactory";
    private final static String EXCHANGE_NAME = "eiffel";

    public void startReceiving() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ConnectionFactory factory = new ConnectionFactory();
                    factory.setHost("localhost");
                    Connection connection = null;
                    connection = factory.newConnection();
                    Channel channel = connection.createChannel();

                    channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
                    String queueName = channel.queueDeclare().getQueue();
                    channel.queueBind(queueName, EXCHANGE_NAME, "");
                    //channel.queueDeclare(QUEUE_NAME, false, false, false, null);

                    DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                        String message = new String(delivery.getBody(), "UTF-8");
                        String res = new Timestamp(new Date().getTime()) + " : " + message + "\n";
                        FileWriter fWriter = new FileWriter ("/tmp/mq.log",  true);
                        PrintWriter writer = new PrintWriter(fWriter);
                        writer.println(res);
                        writer.close();
                    };
                    channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }
}