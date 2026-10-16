import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;

public class NodeServerImpl extends UnicastRemoteObject implements INodeServer {
    private final int nodeId;
    private final LogStore logStore;
    
    private LeaderCoordinator leaderCoordinator; 
    private ElectionManager electionManager;
    private List<String> peerUrls;

    public NodeServerImpl(int nodeId, LogStore logStore) throws RemoteException {
        super();
        this.nodeId = nodeId;
        this.logStore = logStore;
    }

    public void setModules(LeaderCoordinator leaderCoordinator, ElectionManager electionManager) {
        this.leaderCoordinator = leaderCoordinator;
        this.electionManager = electionManager;
    }

    public void setPeers(List<String> peerUrls) {
        this.peerUrls = peerUrls;
    }

    @Override
    public void receivePropose(int epoch, int seq, String op, int value) throws RemoteException {
        logStore.appendEntry(new LogStore.LogEntry(epoch, seq, op, value, "PROPOSED"));
        System.out.println("Node " + nodeId + " logged proposal (" + epoch + ", " + seq + ")");
        
        if (peerUrls != null) {
            for (String url : peerUrls) {
                new Thread(() -> {
                    try {
                        INodeServer stub = (INodeServer) java.rmi.Naming.lookup(url);
                        stub.receiveAck(epoch, seq, nodeId);
                    } catch (Exception e) { /* Peer offline, ignore */ }
                }).start();
            }
        }
    }

    @Override
    public void receiveAck(int epoch, int seq, int fromNodeId) throws RemoteException {
        if (leaderCoordinator != null) {
            leaderCoordinator.checkQuorumAndCommit(epoch, seq);
        }
    }

    @Override
    public void receiveCommit(int epoch, int seq) throws RemoteException {
        logStore.updateStatus(epoch, seq, "COMMITTED");
        System.out.println("Node " + nodeId + " committed transaction (" + epoch + ", " + seq + ")");
    }

    @Override
    public void forwardRequest(String op, int value, String fromClient) throws RemoteException {
        if (leaderCoordinator != null) {
            leaderCoordinator.initiateProposal(op, value);
        } else {
            System.out.println("Node " + nodeId + " forwarding " + op + " to Leader.");
            if (peerUrls != null) {
                for (String url : peerUrls) {
                    new Thread(() -> {
                        try {
                            INodeServer stub = (INodeServer) java.rmi.Naming.lookup(url);
                            stub.receiveForwardedRequest(op, value, fromClient);
                        } catch (Exception e) { /* Peer offline */ }
                    }).start();
                }
            }
        }
    }

    @Override
    public void receiveForwardedRequest(String op, int value, String fromClient) throws RemoteException {
        if (leaderCoordinator != null) {
            leaderCoordinator.initiateProposal(op, value);
        }
    }

    @Override
    public int getTarget() throws RemoteException {
        LogStore.LogEntry latest = logStore.getLatestCommittedEntry();
        return (latest != null) ? latest.value : -1;
    }

    @Override
    public void receiveHeartbeat(int epoch, int fromNodeId) throws RemoteException {
        if (electionManager != null) electionManager.resetHeartbeatTimeout();
    }

    @Override
    public void receiveElection(int epoch, int seq, int fromNodeId) throws RemoteException {
        if (electionManager != null) electionManager.handleElectionMessage(epoch, seq, fromNodeId);
    }
}