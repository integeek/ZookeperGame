import java.io.Serializable;

public class LogEntry implements Serializable {
    private static final long serialVersionUID = 1L;

    public final int epoch;
    public final int seq;
    public final Op op;
    public final int value;
    public Status status;

    public LogEntry(int epoch, int seq, Op op, int value, Status status) {
        this.epoch = epoch;
        this.seq = seq;
        this.op = op;
        this.value = value;
        this.status = status;
    }

    /** Une ligne de fichier : epoch;seq;op;value;status */
    public String toLine() {
        return epoch + ";" + seq + ";" + op + ";" + value + ";" + status;
    }

    public static LogEntry fromLine(String line) {
        String[] p = line.split(";");
        return new LogEntry(Integer.parseInt(p[0]), Integer.parseInt(p[1]),
                Op.valueOf(p[2]), Integer.parseInt(p[3]), Status.valueOf(p[4]));
    }

    @Override
    public String toString() {
        return "(" + epoch + "," + seq + ") " + op + " " + value + " " + status;
    }
}
