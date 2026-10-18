import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class LeaderCoordinator {
    private final NodeImpl localNode;
    private final ExecutorService pool = Executors.newCachedThreadPool();
    private int nextSeq = 0;
    private final ScheduledExecutorService roundTimer = Executors.newSingleThreadScheduledExecutor();
    private final Map<Integer, Integer> guesses = new LinkedHashMap<>();
    private ScheduledFuture<?> roundTask;

    private static final long ACK_TIMEOUT_MS = 2000;
    private static final long ROUND_DURATION_MS = 5000;

    @FunctionalInterface
    private interface PeerCall {
        boolean run(NodeServer peer) throws Exception;
    }

    public LeaderCoordinator(NodeImpl localNode) {
        this.localNode = localNode;
    }

    /**
     * Abandons the current round by clearing all guesses and preventing the task from executing.
     */
    private void cancelCurrentRound() {
        guesses.clear();
        if (roundTask != null) {
            roundTask.cancel(false);
            roundTask = null;
        }
    }

    public synchronized void resetRound() {
        nextSeq = 0;
        cancelCurrentRound();
    }

    /**
     * Processes a client entry.
     * Only values from 0 to 100 are accepted.
     * @param valueToWrite the value to be written.
     * @return the value if the entry is committed, -1 otherwise.
     */
    public synchronized int writeAndReplicate(int valueToWrite) {
        if (valueToWrite < 0 || valueToWrite > 100) {
            return -1;
        }
        cancelCurrentRound();
        return replicateOperation(Op.WRITE, valueToWrite);
    }


    /**
     * Record a client's guess for the current round. Upon the first guess, start a 5-second timer.
     * 2 possible rejections: values outside the limits or no committed target.
     * @param clientId Id of the customer who made the guess.
     * @param guessValue valeur envoyée
     * @return the round number, -1 if there is a problem.
     */
    public synchronized int submitGuess(int clientId, int guessValue) {
        if (guessValue < 0 || guessValue > 100 || localNode.getTarget() == -1) {
            return -1;
        }
        guesses.put(clientId, guessValue);
        if (roundTask == null) {
            final int epochAtStart = localNode.getEpoch();
            roundTask = roundTimer.schedule(() -> closeRound(epochAtStart), ROUND_DURATION_MS, TimeUnit.MILLISECONDS);
        }
        System.out.println("Guess " + guessValue + " from client " + clientId + " (" + guesses.size() + " this round)");
        return localNode.getRound();
    }

    /**
     * Method called 5 seconds after the first guess.
     * Designates the round winner (the guess closest to the target) and sets it as the new committed value.
     * The round is abandoned if there are no guesses, if the node is no longer the leader, or if the guesses date from a different term.
     * @param epochAtStart epoch at the start of the round
     */
    private synchronized void closeRound(int epochAtStart) {
        roundTask = null;
        if (guesses.isEmpty() || !localNode.isLeader() || localNode.getEpoch() != epochAtStart) {
            guesses.clear();
            return;
        }
        int targetValue = localNode.getTarget();
        int winningGuess = -1;
        int smallestDistance = Integer.MAX_VALUE;
        for (int guess : guesses.values()) {
            int distance = Math.abs(guess - targetValue);
            //The winner is the one closest to the target. In the event of a tie, the person with the lower guess wins.
            if (distance < smallestDistance || (distance == smallestDistance && guess < winningGuess)) {
                winningGuess = guess;
                smallestDistance = distance;
            }
        }
        System.out.println("Round closed: guesses=" + guesses + ", target=" + targetValue + " -> winner " + winningGuess);
        guesses.clear();
        replicateOperation(Op.GUESS, winningGuess);
    }

    /**
     * The leader first logs the proposed entry in its own log.
     * Then, it propose to all followers in parallel and returns a count of 1.
     * Once the quorum is reached, the leader commits locally and then propagates the commit to the followers.
     * @param op operation (WRITE or GUESS).
     * @param operationValue value sent by the client.
     * @return operationValue if all goes well, -1 otherwise.
     */
    public synchronized int replicateOperation(Op op, int operationValue) {
        final int currentEpoch = localNode.getEpoch();
        final int seq = ++nextSeq;

        localNode.log.appendLogEntry(new LogEntry(currentEpoch, seq, op, operationValue, Status.PROPOSED));
        System.out.println("Leader " + localNode.nodeId + " PROPOSED (epoch = " + currentEpoch + ", seq = " + seq + ") " + op + " " + operationValue);

        int acknowledgementCount = 1 + executeOnFollowers("PROPOSE (epoch = " + currentEpoch + ", seq = " + seq + ")",
                peer -> peer.propose(currentEpoch, seq, op, operationValue) == 1);

        if (acknowledgementCount < localNode.quorum) {
            System.out.println("No quorum for (epoch = " + currentEpoch + ", seq = " + seq + "): " + acknowledgementCount + "/" + localNode.totalNumberOfNodes);
            return -1;
        }

        localNode.log.markEntryAsCommitted(currentEpoch, seq);
        executeOnFollowers("COMMIT (epoch = " + currentEpoch + ", seq = " + seq + ")", p -> p.commit(currentEpoch, seq) == 1);
        System.out.println("Leader " + localNode.nodeId + " COMMITTED (epoch = " + currentEpoch + ", seq = " + seq + ") -> target = " + operationValue);
        return operationValue;
    }

    /**
     * Execute the same action on all nodes simultaneously and count the number of ACKs.
     * @param operationLabel operation description.
     * @param peerCall operation to be performed on each remote node.
     * @return the number of nodes that have successfully executed the operation.
     */
    private int executeOnFollowers(String operationLabel, PeerCall peerCall) {
        List<Callable<Boolean>> followerTasks = new ArrayList<>();
        for (int id = 1; id <= localNode.totalNumberOfNodes; id++) {
            if (id == localNode.nodeId) {
                continue;
            }
            final int peerId = id;
            followerTasks.add(() -> {
                try {
                    boolean operationSucceeded = peerCall.run(localNode.getPeer(peerId));
                    System.out.println(operationLabel + " -> Node " + peerId + " : " + (operationSucceeded ? "Ok" : "refused"));
                    return operationSucceeded;
                } catch (Exception e) {
                    System.out.println(operationLabel + " -> Node " + peerId + " : unreachable");
                    return false;
                }
            });
        }

        int successfulAcknowledgmentCount = 0;
        try {
            for (Future<Boolean> future : pool.invokeAll(followerTasks, ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
                if (future.isCancelled()) {
                    continue;
                }
                try {
                    if (future.get()) {
                        successfulAcknowledgmentCount++;
                    }
                } catch (ExecutionException ignored) {}
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return successfulAcknowledgmentCount;
    }

    /**
     * Synchronize the leader's log with the followers.
     * @return true if the quorum is reached, false otherwise
     */
    public boolean synchronizeFollowers() {
        final int currentEpoch = localNode.getEpoch();
        final List<LogEntry> logSnapshot = localNode.log.createLogSnapshot();
        int acknowledgementCount = 1 + executeOnFollowers("SYNC (epoch = " + currentEpoch + ")",
                peer -> peer.syncLog(currentEpoch, localNode.nodeId, logSnapshot));
        if (acknowledgementCount < localNode.quorum) {
            return false;
        }
        localNode.log.markAllEntriesAsCommitted();
        return true;
    }
}