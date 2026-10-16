import java.rmi.Remote;
import java.rmi.RemoteException;

public interface INodeServer extends Remote {
    // ZAB Protocol
    void receivePropose(int epoch, int seq, String op, int value) throws RemoteException;
    void receiveAck(int epoch, int seq, int fromNodeId) throws RemoteException;
    void receiveCommit(int epoch, int seq) throws RemoteException;

    // Client Routing
    void forwardRequest(String op, int value, String fromClient) throws RemoteException;
    void receiveForwardedRequest(String op, int value, String fromClient) throws RemoteException;
    int getTarget() throws RemoteException;

    // Fault Tolerance & Election
    void receiveHeartbeat(int epoch, int fromNodeId) throws RemoteException;
    void receiveElection(int epoch, int seq, int fromNodeId) throws RemoteException;
}