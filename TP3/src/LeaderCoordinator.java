import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class LeaderCoordinator {
    private static final long ACK_TIMEOUT_MS = 2000;

    @FunctionalInterface
    private interface PeerCall { boolean run(NodeServer peer) throws Exception; }

    private final NodeImpl node;
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private int nextSeq = 0;
    private static final long ROUND_DURATION_MS = 5000;
    private final ScheduledExecutorService roundTimer = Executors.newSingleThreadScheduledExecutor();
    private final Map<Integer, Integer> guesses = new LinkedHashMap<>();
    private ScheduledFuture<?> roundTask;

    public LeaderCoordinator(NodeImpl node) { this.node = node; }

    private void clearRound() {
        guesses.clear();
        if (roundTask != null) { roundTask.cancel(false); roundTask = null; }
    }

    /** Appelé par becomeLeader() : le seq repart de 0 à chaque nouvelle epoch. */
    public synchronized void reset() {
        nextSeq = 0; }

    /** @return la valeur commitée, ou -1 si le quorum n'a pas été atteint. */
    public synchronized int replicate(Op op, int value) {
        final int epoch = node.getEpoch();
        final int seq = ++nextSeq;

        // 1. le leader journalise d'abord (c'est son propre ACK)
        node.log.append(new LogEntry(epoch, seq, op, value, Status.PROPOSED));
        System.out.println("Leader " + node.nodeId + " PROPOSED (" + epoch + "," + seq + ") " + op + " " + value);

        // 2. PROPOSE à tous les followers, ACK = retour 1
        int acks = 1 + callPeers("PROPOSE (" + epoch + "," + seq + ")",
                p -> p.propose(epoch, seq, op, value) == 1);

        if (acks < NodeImpl.QUORUM) {
            System.out.println("No quorum for (" + epoch + "," + seq + "): " + acks + "/" + NodeImpl.NB_NODES);
            return -1;
        }

        // (test de crash, voir plus bas)
        if (System.getProperty("crashBeforeCommit") != null) Runtime.getRuntime().halt(1);

        // 3. quorum atteint : commit local, puis COMMIT aux followers
        node.log.markCommitted(epoch, seq);
        callPeers("COMMIT (" + epoch + "," + seq + ")", p -> p.commit(epoch, seq) == 1);
        System.out.println("Leader " + node.nodeId + " COMMITTED (" + epoch + "," + seq + ") -> target = " + value);
        return value;
    }

    /** Appelle tous les autres nœuds en parallèle et renvoie le nombre de réponses positives avant le timeout. */
    private int callPeers(String label, PeerCall call) {
        List<Callable<Boolean>> tasks = new ArrayList<>();
        for (int id = 1; id <= NodeImpl.NB_NODES; id++) {
            if (id == node.nodeId) continue;
            final int peerId = id;
            tasks.add(() -> {
                try {
                    boolean ok = call.run(node.getPeer(peerId));
                    System.out.println(label + " -> Node " + peerId + " : " + (ok ? "OK" : "refused"));
                    return ok;
                } catch (Exception e) {
                    System.out.println(label + " -> Node " + peerId + " : unreachable");
                    return false;
                }
            });
        }

        int ok = 0;
        try {
            for (Future<Boolean> f : pool.invokeAll(tasks, ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                if (f.isCancelled()) continue;               // timeout dépassé
                try { if (f.get()) ok++; } catch (ExecutionException ignored) {}
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return ok;
    }
}