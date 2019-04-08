import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * Static class responsible for logging messages and errors.
 */
public class RabbitLogger {
    private static PrintWriter errorWriter, logWriter, shutdownWriter;
    private static String errorsPath = "/etc/plugins/logs/lib_java_errors.log";
    private static String rabbitLogsPath = "/etc/plugins/logs/lib_rabbitmq_received.log";
    private static String shutdownPath = "/etc/plugins/logs/lib_shutdown_errors.log";
    static {
        FileWriter errorFileWriter, logFileWriter, shutdownFileWriter;
        try {
            String home= System.getenv("ARTIFACTORY_HOME");
            errorFileWriter = new FileWriter(home + errorsPath,  true);
            logFileWriter = new FileWriter (home + rabbitLogsPath,  true);
            shutdownFileWriter = new FileWriter(home + shutdownPath, true);
            errorWriter = new PrintWriter(errorFileWriter);
            logWriter = new PrintWriter(logFileWriter);
            shutdownWriter = new PrintWriter(shutdownFileWriter);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeRabbitLog(String log) {
        logWriter.println(log);
        logWriter.flush();
    }

    public static void writeJavaError(Throwable e) {
        e.printStackTrace(errorWriter);
        errorWriter.flush();
    }

    public static void writeJavaError(String e) {
        errorWriter.println(e);
        errorWriter.flush();
    }

    public static void writeShutdownError(Throwable e) {
        e.printStackTrace(shutdownWriter);
        shutdownWriter.flush();
    }

    public static void writeShutdownError(String e) {
        shutdownWriter.println(e);
        shutdownWriter.flush();
    }

    public static void closeShutdownWriter() {
        if (shutdownWriter != null ) {
            shutdownWriter.close();
        }
    }

    public static void closeWriters() {
        if (errorWriter != null) {
            errorWriter.close();
            errorWriter = null;
        }
        if (logWriter != null) {
            logWriter.close();
            logWriter = null;
        }
    }
}
