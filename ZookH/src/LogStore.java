import java.io.*;
import java.util.*;

public class LogStore {
    private final String filePath;
    private final List<LogEntry> logEntries;

    public static class LogEntry {
        public int epoch;
        public int seq;
        public String op;
        public int value;
        public String status;

        public LogEntry(int epoch, int seq, String op, int value, String status) {
            this.epoch = epoch;
            this.seq = seq;
            this.op = op;
            this.value = value;
            this.status = status;
        }

        @Override
        public String toString() {
            return epoch + "," + seq + "," + op + "," + value + "," + status;
        }

        public static LogEntry fromString(String line) {
            String[] parts = line.split(",");
            return new LogEntry(
                Integer.parseInt(parts[0]),
                Integer.parseInt(parts[1]),
                parts[2],
                Integer.parseInt(parts[3]),
                parts[4]
            );
        }
    }

    public LogStore(int nodeId) {
        this.filePath = "data/node_" + nodeId + ".txt";
        this.logEntries = new ArrayList<>();
        new File("data").mkdirs();
        loadLogs();
    }

    private void loadLogs() {
        File file = new File(filePath);
        if (!file.exists()) return;
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = reader.readLine()) != null) {
                logEntries.add(LogEntry.fromString(line));
            }
        } catch (IOException e) {
            System.err.println("Error reading logs: " + e.getMessage());
        }
    }

    private synchronized void persistAll() {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, false))) {
            for (LogEntry entry : logEntries) {
                writer.write(entry.toString());
                writer.newLine();
            }
        } catch (IOException e) {
            System.err.println("Error persisting logs: " + e.getMessage());
        }
    }

    public synchronized void appendEntry(LogEntry entry) {
        logEntries.add(entry);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
            writer.write(entry.toString());
            writer.newLine();
        } catch (IOException e) {
            System.err.println("Error appending log: " + e.getMessage());
        }
    }

    public synchronized void updateStatus(int epoch, int seq, String newStatus) {
        for (LogEntry entry : logEntries) {
            if (entry.epoch == epoch && entry.seq == seq) {
                entry.status = newStatus;
                persistAll();
                break;
            }
        }
    }

    public synchronized LogEntry getLatestCommittedEntry() {
        for (int i = logEntries.size() - 1; i >= 0; i--) {
            if ("COMMITTED".equals(logEntries.get(i).status)) {
                return logEntries.get(i);
            }
        }
        return null;
    }

    public synchronized LogEntry getLastLogEntry() {
        return logEntries.isEmpty() ? null : logEntries.get(logEntries.size() - 1);
    }
}