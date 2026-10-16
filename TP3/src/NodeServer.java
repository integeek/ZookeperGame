import java.rmi.*;
import java.util.List;

public interface NodeServer extends Remote {

    int propose(int epoch, int seq, Op op, int value) throws RemoteException;

    boolean ack(int epoch, int seq, int from) throws RemoteException;

    int commit(int epoch, int seq) throws RemoteException;

    boolean heartbeat(int epoch, int from) throws RemoteException;

    void election(int epoch, int seq, int from) throws RemoteException;

    int forward(Op op, int value, int from_client) throws RemoteException;

    boolean syncLog(int newEpoch, int leaderId, List<LogEntry> leaderLog) throws RemoteException;

    int getTarget() throws RemoteException;
}
