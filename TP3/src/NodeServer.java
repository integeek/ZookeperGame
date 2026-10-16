import java.rmi.*;

public interface NodeServer extends Remote {

    int propose(int epoch, int seq, int op, int value) throws RemoteException;

    boolean ack(int epoch, int seq, int from) throws RemoteException;

    int commit(int epoch, int seq) throws RemoteException;

    boolean heartbeat(int epoch, int from) throws RemoteException;

    void election(int epoch, int seq, int from) throws RemoteException;

    void forward(int op, int value, int from_client) throws RemoteException;
}
