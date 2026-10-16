import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class ElectionManager {
    private final int nodeId;
    private final LogStore logStore;
    private final List<String> peerUrls;
    
    private ScheduledExecutorService timeoutExecutor;
    private final AtomicLong lastHeartbeatTime = new AtomicLong(System.currentTimeMillis());
    private static final long TIMEOUT_MS = 1500;
    private boolean isLeader = false;
    private final Runnable onBecomeLeader;

    public ElectionManager(int nodeId, LogStore logStore, List<String> peerUrls, Runnable onBecomeLeader) {
        this.nodeId = nodeId;
        this.logStore = logStore;
        this.peerUrls = peerUrls;
        this.onBecomeLeader = onBecomeLeader;
    }

    public void startHeartbeatMonitor() {
        timeoutExecutor = Executors.newSingleThreadScheduledExecutor();
        timeoutExecutor.scheduleAtFixedRate(() -> {
            if (!isLeader && System.currentTimeMillis() - lastHeartbeatTime.get() > TIMEOUT_MS) {
                System.out.println("Node " + nodeId + " timed out. Starting election.");
                startElection();
            }
        }, TIMEOUT_MS, 500, TimeUnit.MILLISECONDS);
    }

    public void resetHeartbeatTimeout() {
        lastHeartbeatTime.set(System.currentTimeMillis());
    }

    private void startElection() {
        LogStore.LogEntry lastEntry = logStore.getLastLogEntry();
        int myEpoch = (lastEntry != null) ? lastEntry.epoch : 0;
        int mySeq = (lastEntry != null) ? lastEntry.seq : 0;

        for (String url : peerUrls) {
            new Thread(() -> {
                try {
                    INodeServer stub = (INodeServer) java.rmi.Naming.lookup(url);
                    stub.receiveElection(myEpoch, mySeq, nodeId);
                } catch (Exception e) { /* Peer offline */ }
            }).start();
        }
        handleElectionMessage(myEpoch, mySeq, nodeId);
    }

    public synchronized void handleElectionMessage(int candidateEpoch, int candidateSeq, int candidateId) {
        LogStore.LogEntry lastEntry = logStore.getLastLogEntry();
        int myEpoch = (lastEntry != null) ? lastEntry.epoch : 0;
        int mySeq = (lastEntry != null) ? lastEntry.seq : 0;

        boolean iAmBetter = false;
        
        if (myEpoch > candidateEpoch) { iAmBetter = true; } 
        else if (myEpoch == candidateEpoch) {
            if (mySeq > candidateSeq) { iAmBetter = true; } 
            else if (mySeq == candidateSeq) {
                if (nodeId < candidateId) { iAmBetter = true; }
            }
        }

        if (!iAmBetter && candidateId != nodeId) {
            resetHeartbeatTimeout();
        } else if (!iAmBetter && candidateId == nodeId) {
            becomeLeader();
        }
    }

    private void becomeLeader() {
        if (!isLeader) {
            isLeader = true;
            System.out.println("Node " + nodeId + " won election! Becoming LEADER.");
            if (timeoutExecutor != null) timeoutExecutor.shutdown();
            onBecomeLeader.run(); 
        }
    }
}