import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class LogStore {
    private final List<LogEntry> entries = new ArrayList<>();
    private final Path file;

    public LogStore(int nodeId) throws IOException {
        this.file = Paths.get("log-node" + nodeId + ".txt");
        if (Files.exists(file)) {
            for (String line : Files.readAllLines(file)) {
                if (!line.isBlank()) entries.add(LogEntry.fromLine(line));
            }
        }
    }

    // --- persistance (appelée sous verrou) ---
    private void persist() {
        try {
            List<String> lines = new ArrayList<>();
            for (LogEntry e : entries) lines.add(e.toLine());
            Path tmp = Paths.get(file + ".tmp");
            Files.write(tmp, lines);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private LogEntry find(int epoch, int seq) {
        for (LogEntry e : entries) {
            if (e.epoch == epoch && e.seq == seq) return e;
        }
        return null;
    }

    private static LogEntry copy(LogEntry e) {
        return e == null ? null : new LogEntry(e.epoch, e.seq, e.op, e.value, e.status);
    }

    // --- écriture ---

    /** Ajoute une entrée (ignorée si (epoch, seq) existe déjà : un PROPOSE rejoué ne duplique pas). */
    public synchronized void append(LogEntry e) {
        if (find(e.epoch, e.seq) != null) return;
        entries.add(copy(e));
        persist();
    }

    /** Passe l'entrée à COMMITTED. Renvoie false si elle est inconnue. */
    public synchronized boolean markCommitted(int epoch, int seq) {
        LogEntry e = find(epoch, seq);
        if (e == null) return false;
        e.status = Status.COMMITTED;
        persist();
        return true;
    }

    /** Utilisé par le nouveau leader une fois la majorité synchronisée. */
    public synchronized void markAllCommitted() {
        for (LogEntry e : entries) e.status = Status.COMMITTED;
        persist();
    }

    /** Utilisé par un follower lors de la synchronisation : il adopte le log du leader. */
    public synchronized void replaceAll(List<LogEntry> newEntries) {
        entries.clear();
        for (LogEntry e : newEntries) entries.add(copy(e));
        persist();
    }

    // --- lecture (on renvoie toujours des copies) ---

    public synchronized LogEntry getLast() {
        return entries.isEmpty() ? null : copy(entries.get(entries.size() - 1));
    }

    public synchronized LogEntry getLastCommitted() {
        for (int i = entries.size() - 1; i >= 0; i--) {
            if (entries.get(i).status == Status.COMMITTED) return copy(entries.get(i));
        }
        return null;
    }

    public synchronized List<LogEntry> snapshot() {
        List<LogEntry> out = new ArrayList<>();
        for (LogEntry e : entries) out.add(copy(e));
        return out;
    }

    public synchronized int size() { return entries.size(); }
}