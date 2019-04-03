import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

public class Logger {
    private PrintWriter errorWriter, logWriter;
    private String errorsPath = "/tmp/java_errors.log";
    private String rabbitLogsPath = "/tmp/rabbit_mq.logs";

    // TODO: convert to singleton

    public Logger() {
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

    public void writeRabbitLog(String log) {
        logWriter.println(log);
        logWriter.close();
    }

    public void writeJavaError(String log) {
        errorWriter.println(log);
        errorWriter.close();
    }
}
