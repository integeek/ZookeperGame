import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LeaderCoordinator {
    private final int nodeId;
    private final LogStore logStore;
    private final List<String> followerUrls;
    private final int quorumSize;

    private int currentEpoch;
    private int currentSeq;
    
    private final Map<Integer, Integer> ackCounts;
    private ScheduledExecutorService heartbeatExecutor;

    private int currentTarget = -1;
    private int closestGuess = -1;
    private int closestDiff = Integer.MAX_VALUE;

    public LeaderCoordinator(int nodeId, LogStore logStore, List<String> followerUrls, int totalNodes, int epoch) {
        this.nodeId = nodeId;
        this.logStore = logStore;
        this.followerUrls = followerUrls;
        this.quorumSize = (totalNodes / 2) + 1; 
        this.currentEpoch = epoch;
        this.currentSeq = 0;
        this.ackCounts = new ConcurrentHashMap<>();
        
        LogStore.LogEntry latest = logStore.getLatestCommittedEntry();
        if (latest != null) this.currentTarget = latest.value;
    }

    public synchronized void initiateProposal(String op, int value) {
        if ("GUESS".equals(op)) {
            handleGuess(value);
            return;
        }

        currentSeq++;
        final int seq = currentSeq;
        final int epoch = currentEpoch;

        logStore.appendEntry(new LogStore.LogEntry(epoch, seq, op, value, "PROPOSED"));
        ackCounts.put(seq, 1); 

        System.out.println("Leader proposing: (" + epoch + ", " + seq + ") " + op + "=" + value);

        for (String url : followerUrls) {
            new Thread(() -> {
                try {
                    INodeServer stub = (INodeServer) java.rmi.Naming.lookup(url);
                    stub.receivePropose(epoch, seq, op, value);
                } catch (Exception e) { /* Follower offline */ }
            }).start();
        }
    }

    private void handleGuess(int guessValue) {
        if (currentTarget == -1) return;
        int diff = Math.abs(currentTarget - guessValue);
        if (diff < closestDiff) {
            closestDiff = diff;
            closestGuess = guessValue;
        }
        System.out.println("Leader registered guess: " + guessValue + ". Best is now: " + closestGuess);
        
        initiateProposal("WRITE", closestGuess);
        currentTarget = closestGuess;
        closestDiff = Integer.MAX_VALUE;
    }

    public synchronized void checkQuorumAndCommit(int epoch, int seq) {
        if (epoch != currentEpoch) return;

        int currentAcks = ackCounts.getOrDefault(seq, 0) + 1;
        ackCounts.put(seq, currentAcks);

        if (currentAcks == quorumSize) {
            System.out.println("Quorum reached for (" + epoch + ", " + seq + "). Committing.");
            logStore.updateStatus(epoch, seq, "COMMITTED");

            for (String url : followerUrls) {
                new Thread(() -> {
                    try {
                        INodeServer stub = (INodeServer) java.rmi.Naming.lookup(url);
                        stub.receiveCommit(epoch, seq);
                    } catch (Exception e) { /* Follower offline */ }
                }).start();
            }
        }
    }

    public void startHeartbeatBroadcast() {
        heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            for (String url : followerUrls) {
                try {
                    INodeServer stub = (INodeServer) java.rmi.Naming.lookup(url);
                    stub.receiveHeartbeat(currentEpoch, nodeId);
                } catch (Exception e) { /* Follower offline */ }
            }
        }, 0, 500, TimeUnit.MILLISECONDS);
    }
}