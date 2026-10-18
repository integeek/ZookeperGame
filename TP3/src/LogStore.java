import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class LogStore {
    private final List<LogEntry> entries = new ArrayList<>();
    private final Path file;

    /**
     * Checks if the file exists, reads each line, and reconstructs the entries
     * @param nodeId Id of the node from which we want to retrieve the logs.
     * @throws IOException
     */
    public LogStore(int nodeId) throws IOException {
        this.file = Paths.get("log-node" + nodeId + ".txt");
        if (Files.exists(file)) {
            for (String logLine : Files.readAllLines(file)) {
                if (!logLine.isBlank()) {
                    entries.add(LogEntry.fromLine(logLine));
                }
            }
        }
    }

    /**
     * Saves logs to the disk.
     * Converts each log entry into a line of text, and writes the lines to a temporary file.
     * then replace the existing file with this temporary file.
     */
    private void persistLogEntries() {
        try {
            List<String> logLines = new ArrayList<>();
            for (LogEntry logEntry : entries) {
                logLines.add(logEntry.toLine());
            }
            Path temporaryFile = Paths.get(file + ".tmp");
            Files.write(temporaryFile, logLines);
            Files.move(temporaryFile, file, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Find an entry using its epoch and sequence.
     * @param epochToFind epoch to search for.
     * @param seqToFind sequence to search for.
     * @return the entry if it exists, null otherwise
     */
    private LogEntry findEntryByEpochAndSequence(int epochToFind, int seqToFind) {
        for (LogEntry logEntry : entries) {
            if (logEntry.epoch == epochToFind && logEntry.seq == seqToFind) {
                return logEntry;
            }
        }
        return null;
    }

    /**
     * Create a copy of an entry.
     * @param logEntry the entry to copy.
     * @return the copied entry.
     */
    private static LogEntry copyLogEntry(LogEntry logEntry) {
        return logEntry == null ? null : new LogEntry(logEntry.epoch, logEntry.seq, logEntry.op, logEntry.value, logEntry.status);
    }

    /**
     * Add an entry if the (epoch, seq) pair does not already exist.
     * @param logEntryToAppend entry to be added.
     */
    public synchronized void appendLogEntry(LogEntry logEntryToAppend) {
        if (findEntryByEpochAndSequence(logEntryToAppend.epoch, logEntryToAppend.seq) != null) {
            return;
        }
        entries.add(copyLogEntry(logEntryToAppend));
        persistLogEntries();
    }

    /**
     * Set an entry to the committed state.
     * Searches for the entry based on its epoch and seq.
     * @param targetEpoch epoch from entry to commit.
     * @param targetSeq sequence from entry to commit.
     * @return true if the entry was found and marked as COMMITTED, false otherwise.
     */
    public synchronized boolean markEntryAsCommitted(int targetEpoch, int targetSeq) {
        LogEntry logEntry = findEntryByEpochAndSequence(targetEpoch, targetSeq);
        if (logEntry == null) {
            return false;
        }
        logEntry.status = Status.COMMITTED;
        persistLogEntries();
        return true;
    }

    /**
     * Set all entries to the COMMITTED state.
     */
    public synchronized void markAllEntriesAsCommitted() {
        for (LogEntry logEntry : entries) {
            logEntry.status = Status.COMMITTED;
        }
        persistLogEntries();
    }

    /**
     * Completely replaces the local log with a provided list of entries.
     * @param newEntries entries to be recorded in the journal.
     */
    public synchronized void replaceAllEntries(List<LogEntry> newEntries) {
        entries.clear();
        for (LogEntry logEntry : newEntries) {
            entries.add(copyLogEntry(logEntry));
        }
        persistLogEntries();
    }

    /**
     * Search for the last validated entry in the journal.
     * Scans the log from the end and returns a copy of the first COMMITTED entry encountered.
     * @return a copy of the last COMMITTED entry, or null if there is none.
     */
    public synchronized LogEntry getLastCommittedEntry() {
        for (int index = entries.size() - 1; index >= 0; index--) {
            if (entries.get(index).status == Status.COMMITTED) {
                return copyLogEntry(entries.get(index));
            }
        }
        return null;
    }

    /**
     * Count the number of committed entries in the log.
     * @return the number of entries whose status is committed.
     */
    public synchronized int countCommittedEntries() {
        int committedEntryCount = 0;
        for (LogEntry logEntry : entries) {
            if (logEntry.status == Status.COMMITTED) {
                committedEntryCount++;
            }
        }
        return committedEntryCount;
    }

    /**
     * Create a copy of the current journal entries.
     * @return a list containing copies of the journal entries.
     */
    public synchronized List<LogEntry> createLogSnapshot() {
        List<LogEntry> logSnapshot = new ArrayList<>();
        for (LogEntry logEntry : entries) {
            logSnapshot.add(copyLogEntry(logEntry));
        }
        return logSnapshot;
    }
}