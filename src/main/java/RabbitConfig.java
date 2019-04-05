import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Class that parses configs from the Artifactory host
 */
public class RabbitConfig {
    private String username, password, vhost, hostname;
    private int port;

    public RabbitConfig() {
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

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getVhost() { return vhost; }

    public String getHostname() { return hostname; }

    public int getPort() {
        return port;
    }
}
