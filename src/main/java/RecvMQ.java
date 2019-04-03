import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.io.*;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

public class RecvMQ {

    private final static String QUEUE_NAME = "eiffelactory";
    private final static String EXCHANGE_NAME = "eiffel";
    private volatile boolean alive = true;

    public void startReceiving() throws IOException {
        FileWriter errorFileWriter = new FileWriter ("/tmp/java.log",  true);
        PrintWriter errorWriter = new PrintWriter(errorFileWriter);

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection connection = initConnection();
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
                        if (!alive) {
                            try {
//                                channel.queueUnbind(queueName, EXCHANGE_NAME, "");
                                channel.close();
                                connection.close();
                            } catch (TimeoutException e) {
                                e.printStackTrace();
                                errorWriter.println(e);
                                errorWriter.close();
                            }
                        }
                    };
                    channel.basicConsume(queueName, true, deliverCallback, consumerTag -> { });
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                    errorWriter.println(e.getMessage() + e.getCause());
                    errorWriter.close();
                }
            }
        }).start();
    }

    private Connection initConnection() throws IOException, TimeoutException {
        FileWriter errorFileWriter = new FileWriter ("/tmp/java.log",  true);
        PrintWriter errorWriter = new PrintWriter(errorFileWriter);
        Properties prop = new Properties();
        FileInputStream config = null;
        String username = null, password = null, vhost = null, hostname = null;
        int port = 0;

        try {
            config = new FileInputStream("/etc/secrets.properties");
            prop.load(config);
            username = prop.getProperty("username");
            password = prop.getProperty("password");
            vhost = prop.getProperty("vhost");
            hostname = prop.getProperty("hostname");
            port = Integer.valueOf(prop.getProperty("port"));
        } catch (IOException ex) {
            errorWriter.println(ex);
            errorWriter.close();
        } finally {
            if (config != null) {
                try {
                    config.close();
                } catch (IOException e) {
                    errorWriter.println(e);
                    errorWriter.close();
                }
            }
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUsername(username);
            factory.setPassword(password);
            factory.setVirtualHost(vhost);
            factory.setHost(hostname);
            factory.setPort(port);
            return factory.newConnection();
        }
    }

    public void stopReceiving() {
        alive = false;
    }
}