import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class LogStore {
    private final List<LogEntry> entries = new ArrayList<>();
    private final Path file;

    /**
     * Vérifie si le fichier existe, lit chaque ligne et reconstruit les entrées
     * @param nodeId
     * @throws IOException
     */
    public LogStore(int nodeId) throws IOException {
        this.file = Paths.get("log-node" + nodeId + ".txt");
        if (Files.exists(file)) {
            for (String line : Files.readAllLines(file)) {
                if (!line.isBlank()) {
                    entries.add(LogEntry.fromLine(line));
                }
            }
        }
    }

    /**
     * Enregistre les logs sur le disque.
     */
    private void persist() {
        try {
            List<String> lines = new ArrayList<>();
            for (LogEntry e : entries) {
                lines.add(e.toLine());
            }
            Path tmp = Paths.get(file + ".tmp");
            Files.write(tmp, lines);
            Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    /**
     * Trouver une entrée grâce à son epoch et sa sequence.
     * @param epoch epoch à rechercher.
     * @param seq séquence à rechercher.
     * @return l'entrée si elle existe, null sinon
     */
    private LogEntry find(int epoch, int seq) {
        for (LogEntry e : entries) {
            if (e.epoch == epoch && e.seq == seq) {
                return e;
            }
        }
        return null;
    }

    /**
     * Créer une copie d'une entrée
     * @param e l'entrée à copier
     * @return l'entrée copiée
     */
    private static LogEntry copy(LogEntry e) {
        return e == null ? null : new LogEntry(e.epoch, e.seq, e.op, e.value, e.status);
    }

    /**
     * Ajoute une entrée si le couple epoch, seq n'existe pas déjà.
     * @param e entrée à ajouter.
     */
    public synchronized void append(LogEntry e) {
        if (find(e.epoch, e.seq) != null) return;
        entries.add(copy(e));
        persist();
    }

    /**
     * Passe une entrée à l'état commited.
     * @param epoch
     * @param seq
     * @return
     */
    public synchronized boolean markCommitted(int epoch, int seq) {
        LogEntry e = find(epoch, seq);
        if (e == null) return false;
        e.status = Status.COMMITTED;
        persist();
        return true;
    }

    public synchronized void markAllCommitted() {
        for (LogEntry e : entries) e.status = Status.COMMITTED;
        persist();
    }

    public synchronized void replaceAll(List<LogEntry> newEntries) {
        entries.clear();
        for (LogEntry e : newEntries) entries.add(copy(e));
        persist();
    }

    public synchronized LogEntry getLast() {
        return entries.isEmpty() ? null : copy(entries.get(entries.size() - 1));
    }

    public synchronized LogEntry getLastCommitted() {
        for (int i = entries.size() - 1; i >= 0; i--) {
            if (entries.get(i).status == Status.COMMITTED) return copy(entries.get(i));
        }
        return null;
    }

    public synchronized int committedCount() {
        int n = 0;
        for (LogEntry e : entries) if (e.status == Status.COMMITTED) n++;
        return n;
    }

    public synchronized List<LogEntry> snapshot() {
        List<LogEntry> out = new ArrayList<>();
        for (LogEntry e : entries) {
            out.add(copy(e));
        }
        return out;
    }

    public synchronized int size() {
        return entries.size();
    }
}