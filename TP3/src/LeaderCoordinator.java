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
     * Abandonne le round en cours en effaçant tous les guess et en empéchant la tâche de s'exécuter.
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
     * Méthode du leader qui traite une écrite d'un client.
     * Seule les valeurs de 0 à 100 sont acceptées.
     * @param valueToWrite la valeur à écrire
     * @return la valeur si l'entrée est commitée, -1 sinon
     */
    public synchronized int writeAndReplicate(int valueToWrite) {
        if (valueToWrite < 0 || valueToWrite > 100) {
            return -1;
        }
        cancelCurrentRound();
        return replicateOperation(Op.WRITE, valueToWrite);
    }


    /**
     * Enreigstre le guess d'un client dans le round en cours. Au premier guess, lance un compteur de 5 secondes.
     * 2 refus possibles : valeurs hors des limites ou pas de target commitée.
     * @param clientId id du client ayant fait le guess
     * @param guessValue valeur envoyée
     * @return le numéro du round, -1 si un problème
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
     * Méthode appelée 5 secodes après le premier guess.
     * Désigne le gagnant du round (le guess le plus proche de la target) et le réplique comme la nouvelle valeur commitée.
     * Round abandonné s'il n'y a pas de guess, le noeud n'est plus leader ou si les guess datent d'un autre mandat.
     * @param epochAtStart
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
            //Le gagnant est le plus proche de la cible, si égalité, c'est le plus petit guess si gagne
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
     * Leader journalise d'abord l'entrée en proposed dans son propre log.
     * Puis appelle propose sur tous les followers en parallèle et renvoie le nombre de 1.
     * Quand le quorum est atteint, le leader commit chez lui puis diffuse le commit aux followers.
     * @param op opération (WRITE ou GUESS).
     * @param operationValue valeur envoyé par le client.
     * @return operationValue si tout va bien, -1 sinon.
     */
    public synchronized int replicateOperation(Op op, int operationValue) {
        final int currentEpoch = localNode.getEpoch();
        final int seq = ++nextSeq;

        localNode.log.appendLogEntry(new LogEntry(currentEpoch, seq, op, operationValue, Status.PROPOSED));
        System.out.println("Leader " + localNode.nodeId + " PROPOSED (epoch = " + currentEpoch + ", seq = " + seq + ") " + op + " " + operationValue);

        int acknowledgementCount = 1 + executeOnFollowers("PROPOSE (epoch = " + currentEpoch + ", seq = " + seq + ")",
                peer -> peer.propose(currentEpoch, seq, op, operationValue) == 1);

        if (acknowledgementCount < NodeImpl.QUORUM) {
            System.out.println("No quorum for (epoch = " + currentEpoch + ", seq = " + seq + "): " + acknowledgementCount + "/" + NodeImpl.NB_NODES);
            return -1;
        }

        if (System.getProperty("crashBeforeCommit") != null) {
            Runtime.getRuntime().halt(1);
        }

        localNode.log.markEntryAsCommitted(currentEpoch, seq);
        executeOnFollowers("COMMIT (epoch = " + currentEpoch + ", seq = " + seq + ")", p -> p.commit(currentEpoch, seq) == 1);
        System.out.println("Leader " + localNode.nodeId + " COMMITTED (epoch = " + currentEpoch + ", seq = " + seq + ") -> target = " + operationValue);
        return operationValue;
    }

    /**
     * Executer la même action sur tous les noeuds en simultanné et compte le nombre de ACK.
     * @param operationLabel libbelé de l'operation
     * @param peerCall opération à exécuter sur chaque noeud distant
     * @return le nombre de noeuds ayant executé l'opération avec succès
     */
    private int executeOnFollowers(String operationLabel, PeerCall peerCall) {
        List<Callable<Boolean>> followerTasks = new ArrayList<>();
        for (int id = 1; id <= NodeImpl.NB_NODES; id++) {
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
     * Synchronise le journal du leader avec les followers.
     * @return true si le quorum est atteint, false sinon
     */
    public boolean synchronizeFollowers() {
        final int currentEpoch = localNode.getEpoch();
        final List<LogEntry> logSnapshot = localNode.log.createLogSnapshot();
        int acknowledgementCount = 1 + executeOnFollowers("SYNC (epoch = " + currentEpoch + ")",
                peer -> peer.syncLog(currentEpoch, localNode.nodeId, logSnapshot));
        if (acknowledgementCount < NodeImpl.QUORUM) {
            return false;
        }
        localNode.log.markAllEntriesAsCommitted();
        return true;
    }
}