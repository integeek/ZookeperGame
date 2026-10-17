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
     * @param nodeId id du noeud duquel on veut récupérer les logs
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
     * Enregistre les logs sur le disque.
     * Convertit chaque entrée du journal en une ligne de texte, écrit les lignes dans un fichier temporaire
     * puis remplace le fichier existant par ce fichier temporaire
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
     * Trouver une entrée grâce à son epoch et sa sequence.
     * @param epochToFind epoch à rechercher.
     * @param seqToFind séquence à rechercher.
     * @return l'entrée si elle existe, null sinon
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
     * Créer une copie d'une entrée
     * @param logEntry l'entrée à copier
     * @return l'entrée copiée
     */
    private static LogEntry copyLogEntry(LogEntry logEntry) {
        return logEntry == null ? null : new LogEntry(logEntry.epoch, logEntry.seq, logEntry.op, logEntry.value, logEntry.status);
    }

    /**
     * Ajoute une entrée si le couple epoch, seq n'existe pas déjà.
     * @param logEntryToAppend entrée à ajouter.
     */
    public synchronized void appendLogEntry(LogEntry logEntryToAppend) {
        if (findEntryByEpochAndSequence(logEntryToAppend.epoch, logEntryToAppend.seq) != null) {
            return;
        }
        entries.add(copyLogEntry(logEntryToAppend));
        persistLogEntries();
    }

    /**
     * Passe une entrée à l'état commited.
     * Recherche l'entrée à partir de son epoch et de son seq.
     * @param targetEpoch epoch de l'entrée à commit
     * @param targetSeq seq de l'entrée à commit
     * @return true si l'entrée à été trouvée et marquée comme validée, false sinon
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
     * Passe toutes les entrées à l'état commited.
     */
    public synchronized void markAllEntriesAsCommitted() {
        for (LogEntry logEntry : entries) {
            logEntry.status = Status.COMMITTED;
        }
        persistLogEntries();
    }

    /**
     * Remplace intégralement le log local par une liste d'entrées fournie.
     * @param newEntries les entrées à enregistrer dans le journal.
     */
    public synchronized void replaceAllEntries(List<LogEntry> newEntries) {
        entries.clear();
        for (LogEntry logEntry : newEntries) {
            entries.add(copyLogEntry(logEntry));
        }
        persistLogEntries();
    }

    /**
     * Recherche la dernière entrée validée dans le journal.
     * Parcourt le journal depuis la fin et renvoie une copie de la première entrée COMMITED rencontrée.
     * @return une copie de la dernière entrée validée ou null s'il n'y en a pas.
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
     * Compte le nombre d'entrées commited dans le journal
     * @return le nombre d'entrée dont le statut est commited
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
     * Crée une copie des entrées actuelles du journal.
     * @return une liste contenant des copies des entrées du journal
     */
    public synchronized List<LogEntry> createLogSnapshot() {
        List<LogEntry> logSnapshot = new ArrayList<>();
        for (LogEntry logEntry : entries) {
            logSnapshot.add(copyLogEntry(logEntry));
        }
        return logSnapshot;
    }
}