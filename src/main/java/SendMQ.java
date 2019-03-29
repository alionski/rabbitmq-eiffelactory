import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class SendMQ {

    private final static String QUEUE_NAME = "eiffelactory";
    private final static String EXCHANGE_NAME = "eiffel";

	public void send(String message) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
            //channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes("UTF-8"));
		 } catch (Exception e) {
            System.out.println(e);
        }
	 }

    public static void main(String[] argv) throws Exception { }
}
