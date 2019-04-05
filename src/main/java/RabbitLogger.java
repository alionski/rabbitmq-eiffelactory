import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class RabbitLogger {
    private static PrintWriter errorWriter, logWriter;
    private static String errorsPath = "/tmp/java_errors.log";
    private static String rabbitLogsPath = "/tmp/rabbitmq.logs";
    static {
        FileWriter errorFileWriter, logFileWriter;
        try {
            errorFileWriter = new FileWriter(errorsPath,  true);
            logFileWriter = new FileWriter (rabbitLogsPath,  true);
            errorWriter = new PrintWriter(errorFileWriter);
            logWriter = new PrintWriter(logFileWriter);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeRabbitLog(String log) {
        logWriter.println(log);
        logWriter.flush();
    }

    public static void writeJavaError(Exception e) {
        e.printStackTrace(errorWriter);
        errorWriter.flush();
    }
}
