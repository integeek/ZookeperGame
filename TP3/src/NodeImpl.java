import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class NodeImpl extends UnicastRemoteObject implements NodeServer {
    int nodeId;
    int epoch = 1;
    int leaderId = 0;
    long lastHeartbeat = 0;
    int seq = 0;

    int bestCandidateId = -1;
    int bestCandidateEpoch = -1;
    int bestCandidateSeq = -1;

    final int totalNumberOfNodes;
    final int quorum;
    volatile boolean electionStarted = false;
    volatile boolean leaderReady = false;
    private ScheduledFuture<?> heartbeatTask;
    final LeaderCoordinator coordinator = new LeaderCoordinator(this);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    final LogStore log;

    private static final long TIME_BETWEEN_HEARTBEAT = 10;
    private static final long HEARTBEAT_TIMEOUT_BEFORE_ELECTION = 15_000;
    private static final long ELECTION_DURATION = 2;


    public NodeImpl(int nodeId, int totalNumberOfNodes) throws IOException {
        super();
        this.nodeId = nodeId;
        this.lastHeartbeat = System.currentTimeMillis();
        this.log = new LogStore(nodeId);
        this.totalNumberOfNodes = totalNumberOfNodes;
        this.quorum = totalNumberOfNodes / 2 + 1;
    }

    synchronized int getEpoch() {
        return epoch;
    }

    boolean isLeader() {
        return leaderReady && leaderId == nodeId;
    }

    /**
     * Allows a node to be retrieved using its Id.
     * It then allows for calling methods remotely on a peer.
     * @param id Id of the node that we want to return.
     * @return the RMI stub corresponding to the Id.
     * @throws Exception
     */
    NodeServer getPeer(int id) throws Exception {
        return (NodeServer) LocateRegistry.getRegistry("localhost", 5000 + id).lookup("Node" + id);
    }

    /**
     * Starts the node's operation, node 1 is initially the leader.
     * If the server is a leader, it sends heartbeats to the other servers every 10 seconds.
     * Otherwise, the follower checks every second when the last heartbeat occurred.
     */
    public void start() {
        if (nodeId == 1) {
            leaderId = nodeId;
            leaderReady = true;
            System.out.println("Node 1 is the leader");
            scheduler.scheduleAtFixedRate(this::sendHeartBeats, 0, TIME_BETWEEN_HEARTBEAT, TimeUnit.SECONDS);
        } else {
            scheduler.scheduleAtFixedRate(this::checkHeartbeat, 1, 1, TimeUnit.SECONDS);
        }
    }

    /**
     * For the leader.
     * Sends a heartbeat to every node in the network to signal that it is alive and to communicate its epoch and Id.
     * Also displays whether the heartbeat is received.
     */
    void sendHeartBeats() {
        for (int id = 1; id <= totalNumberOfNodes; id++) {
            if (id == nodeId) {
                continue;
            }
            try {
                boolean heartbeatAccepted = getPeer(id).heartbeat(epoch, nodeId);
                System.out.println("Heartbeat sent to Node " + id + ", accepted = " + heartbeatAccepted);
            } catch (Exception e) {
                System.out.println("Cannot contact Node " + id);
            }
        }
    }

    /**
     * For the followers.
     * The leader sends a proposal for an operation, and each follower responds whether they agree to log it.
     * Create a log entry with the status PROPOSED if the proposal does not originate from an outdated leader.
     * @param proposeEpoch epoch of the leader who issues the proposal
     * @param proposeSeq sequence number of the proposal within the leader's epoch
     * @param op operation to be logged (WRITE or GUESS)
     * @param value value associated with the operation
     * @return 1 if the proposal has been logged (equivalent to ACK), 0 otherwise
     * @throws RemoteException
     */
    @Override
    public int propose(int proposeEpoch, int proposeSeq, Op op, int value) throws RemoteException {
        synchronized (this) {
            if (proposeEpoch < epoch) {
                return 0;
            }
        }
        log.appendLogEntry(new LogEntry(proposeEpoch, proposeSeq, op, value, Status.PROPOSED));
        System.out.println("Node " + nodeId + " logged PROPOSED (epoch = " + proposeEpoch + ", seq = " + proposeSeq + ") for " + op + "(" + value + ")");
        return 1;
    }

    /**
     * For the followers.
     * LogStore searches for the entry identified by the pair (epoch, seq) and updates its status from PROPOSED to COMMITTED.
     * @param commitEpoch epoch de la proposition
     * @param commitSeq proposal epoch
     * @return 1 if the commit was successful, 0 if it failed.
     * @throws RemoteException
     */
    @Override
    public int commit(int commitEpoch, int commitSeq) throws RemoteException {
        boolean commitSuccessful = log.markEntryAsCommitted(commitEpoch, commitSeq);
        if (commitSuccessful) {
            System.out.println("Node " + nodeId + " COMMITTED (epoch = " + commitEpoch + ", seq = " + commitSeq + ")");
        }
        return commitSuccessful ? 1 : 0;
    }

    /**
     * Retrieves the value of the last validated operation in the journal.
     * @return the value of the last validated entry, -1 otherwise.
     */
    @Override
    public int getTarget() {
        LogEntry lastCommitedEntry = log.getLastCommittedEntry();
        return lastCommitedEntry == null ? -1 : lastCommitedEntry.value;
    }

    /**
     * For the followers.
     * Reject heartbeats originating from an earlier epoch, or if the node is the leader and receives a heartbeat from a node with a higher Id.
     * If accepted, it updates the known leader, the epoch, the date of receipt, and the election state.
     * The node also stops its task of sending heartbeats.
     * @param heartbeatEpoch the epoch of the heartbeat sent by the leader.
     * @param from : leader Id.
     * @return true if the heartbeat is accepted, false otherwise.
     * @throws RemoteException
     */
    @Override
    public synchronized boolean heartbeat(int heartbeatEpoch, int from) {
        if (heartbeatEpoch < epoch) {
            return false;
        }

        if (heartbeatEpoch == epoch && leaderId == nodeId && from > nodeId) {
            return false;
        }

        if (leaderId == nodeId && heartbeatTask != null) {
            heartbeatTask.cancel(false);
            heartbeatTask = null;
            leaderReady = false;
        }
        epoch = heartbeatEpoch;
        leaderId = from;
        lastHeartbeat = System.currentTimeMillis();
        electionStarted = false;
        System.out.println("Node " + nodeId + " received heartbeat from Node " + from);
        return true;
    }

    /**
     * For the followers.
     * Check whether the leader is still sending heartbeats.
     * If the last heartbeat occurred more than HEARTBEAT_TIMEOUT_BEFORE_ELECTION seconds ago, trigger an election, provided one is not already underway.
     */
    void checkHeartbeat() {
        if (leaderId == nodeId) {
            return;
        }

        long timeElapsedSinceLastHeartbeat = System.currentTimeMillis() - lastHeartbeat;
        if (timeElapsedSinceLastHeartbeat > HEARTBEAT_TIMEOUT_BEFORE_ELECTION && !electionStarted) {
            System.out.println("Node " + nodeId + " : no heartbeat received for " + HEARTBEAT_TIMEOUT_BEFORE_ELECTION + " milliseconds");
            startElection();
        }
    }

    /**
     * Start an election.
     * The node initially declares itself the best candidate, stores its state, broadcasts its candidacy, and then schedules the termination.
     */
    void startElection() {
        int currentEpoch;
        int currentSequence;
        synchronized (this) {
            if (electionStarted) {
                return;
            }
            electionStarted = true;
            bestCandidateEpoch = epoch;
            bestCandidateSeq = seq;
            bestCandidateId = nodeId;
            currentEpoch = epoch;
            currentSequence = seq;
        }

        broadcastCandidacy(currentEpoch, currentSequence);
        scheduler.schedule(this::finishElection, ELECTION_DURATION, TimeUnit.SECONDS);
    }

    /**
     * Compare a candidate with the best candidate currently known.
     * The order of priority is :
     * 1. The highest epoch.
     * 2. The highest sequence number if the epochs are equal.
     * 3. The smallest identifier if the two preceding values are equal.
     * @param candidateEpoch epoch of the candidate.
     * @param candidateSeq sequence number of the candidate.
     * @param candidateId Candidate identifier.
     * @return true if the candidate is better than the current candidate, false otherwise.
     */
    private boolean isBetterCandidate(int candidateEpoch, int candidateSeq, int candidateId) {
        if (candidateEpoch != bestCandidateEpoch) {
            return candidateEpoch > bestCandidateEpoch;
        }

        if (candidateSeq != bestCandidateSeq) {
            return candidateSeq > bestCandidateSeq;
        }

        return candidateId < bestCandidateId;
    }

    /**
     * For the followers.
     * Receives an application from another node.
     * If this node is not yet participating in an election, it joins it, declares itself a candidate, and schedules the election's conclusion.
     * He then compares the candidate who has been received with the best known candidate.
     * @param candidateEpoch epoch announced by the candidate.
     * @param candidateSeq sequence number announced by the candidate.
     * @param from candidate node identifier.
     */
    @Override
    public synchronized void election(int candidateEpoch, int candidateSeq, int from) {
        System.out.println("Node " + nodeId + " received election from Node " + from);
        boolean joined = false;
        if (!electionStarted) {
            electionStarted = true;
            joined = true;
            bestCandidateEpoch = epoch;
            bestCandidateSeq = seq;
            bestCandidateId = nodeId;
            scheduler.schedule(this::finishElection, ELECTION_DURATION, TimeUnit.SECONDS);
        }

        if (isBetterCandidate(candidateEpoch, candidateSeq, from)) {
            bestCandidateEpoch = candidateEpoch;
            bestCandidateSeq = candidateSeq;
            bestCandidateId = from;
        }
        System.out.println("Node " + nodeId + " best candidate = Node " + bestCandidateId);

        if (joined) {
            final int currentEpoch = epoch;
            final int currentSequence = seq;
            scheduler.execute(() -> broadcastCandidacy(currentEpoch, currentSequence));
        }
    }

    /**
     * Broadcasts this node's candidacy to all other nodes.
     * Each node receives the epoch, the sequence number, and the candidate's identifier in order to compare the candidacies.
     * @param candidateEpoch candidate epoch.
     * @param candidateSequence candidate sequence number.
     */
    void broadcastCandidacy(int candidateEpoch, int candidateSequence) {
        for (int id = 1; id <= totalNumberOfNodes; id++) {
            if (id == nodeId) {
                continue;
            }
            try {
                getPeer(id).election(candidateEpoch, candidateSequence, nodeId);
            }
            catch (Exception ex) {
                System.out.println("Cannot contact Node " + id);
            }
        }
    }

    /**
     * Conclude the election and determine whether this node becomes the leader.
     * If the node is the best-known candidate, it becomes the leader.
     * Otherwise, he recognizes the best candidate as the leader.
     */
    void finishElection() {
        boolean wonElection;
        synchronized (this) {
            System.out.println("Node " + nodeId + " election result: Node " + bestCandidateId);
            wonElection = (bestCandidateId == nodeId);
            if (!wonElection) {
                leaderId = bestCandidateId;
                lastHeartbeat = System.currentTimeMillis();
                electionStarted = false;
            }
        }
        if (wonElection) {
            becomeLeader();
        } else {
            System.out.println("Node " + nodeId + " recognizes Node " + leaderId + " as leader");
        }
    }

    /**
     * Promote this node to the leader role.
     * Increments the epoch by 1 and resets the sequence to 0.
     * It also sends its logs to all its peers and waits for a quorum of ACKs.
     */
    void becomeLeader() {
        synchronized (this) {
            epoch++;
            seq = 0;
            leaderId = nodeId;
            electionStarted = false;
        }
        coordinator.resetRound();
        if (!coordinator.synchronizeFollowers()) {
            return;
        }
        leaderReady = true;
        sendHeartBeats();
        heartbeatTask = scheduler.scheduleAtFixedRate(this::sendHeartBeats, TIME_BETWEEN_HEARTBEAT, TIME_BETWEEN_HEARTBEAT, TimeUnit.SECONDS);
    }

    /**
     * The leader invokes this method on each follower to impose its log upon them.
     * Replace the local log with the leader's log, mark everything as COMMITTED, and adopt the epoch and the leader.
     * @param newEpoch new epoch.
     * @param from Id of the new leader.
     * @param leaderLog Leader's log.
     * @return true if no error, false if the leader is outdated.
     */
    @Override
    public synchronized boolean syncLog(int newEpoch, int from, List<LogEntry> leaderLog) {
        if (newEpoch < epoch) {
            return false;
        }
        log.replaceAllEntries(leaderLog);
        log.markAllEntriesAsCommitted();
        epoch = newEpoch;
        leaderId = from;
        lastHeartbeat = System.currentTimeMillis();
        electionStarted = false;
        return true;
    }

    /**
     * If the node is the leader, it processes the request.
     * Otherwise, he passes it on to the leader.
     * @param op operation received by the node
     * @param value value received by the node
     * @param fromClient original client.
     * @return For write, the value is committed, for guess, the round number is provided, and -1 if rejection.
     * @throws RemoteException
     */
    @Override
    public int forward(Op op, int value, int fromClient) throws RemoteException {
        System.out.println("Node " + nodeId + " received " + op + "(" + value + ") from client " + fromClient);

        if (isLeader()) {
            return op == Op.WRITE ? coordinator.writeAndReplicate(value) : coordinator.submitGuess(fromClient, value);
        }

        int knownLeaderId = leaderId;
        if (knownLeaderId == 0 || knownLeaderId == nodeId) {
            System.out.println("No leader available");
            return -1;
        }
        try {
            return getPeer(knownLeaderId).forward(op, value, fromClient);
        } catch (Exception e) {
            System.out.println("Cannot forward to leader Node " + knownLeaderId);
            return -1;
        }
    }

    @Override
    public int getRound() {
        return log.countCommittedEntries();
    }
}
