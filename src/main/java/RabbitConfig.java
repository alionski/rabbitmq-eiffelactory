import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Class that parses configs from the Artifactory host
 */
public class RabbitConfig {
    private static String username, password, vhost, hostname, exchange, queue, exchangeType;
    private static int port;
    static {
        Properties prop = new Properties();
        FileInputStream config = null;

        try {
            config = new FileInputStream("/etc/secrets.properties");
            prop.load(config);
            username = prop.getProperty("username");
            password = prop.getProperty("password");
            vhost = prop.getProperty("vhost");
            hostname = prop.getProperty("hostname");
            port = Integer.valueOf(prop.getProperty("port"));
            exchange = prop.getProperty("exchange");
            queue = prop.getProperty("queue");
            exchangeType = prop.getProperty("exchange_type");
        } catch (IOException e) {
            RabbitLogger.writeJavaError(e);
        } finally {
            if (config != null) {
                try {
                    config.close();
                } catch (IOException e) {
                    RabbitLogger.writeJavaError(e);
                }
            }
        }
    }

    public static String getExchange() { return exchange; }

    public static String getQueue() { return queue; }

    public static String getExchangeType() { return exchangeType; }

    public static String getUsername() { return username; }

    public static  String getPassword() { return password; }

    public static String getVhost() { return vhost; }

    public static String getHostname() { return hostname; }

    public static int getPort() { return port; }
}
