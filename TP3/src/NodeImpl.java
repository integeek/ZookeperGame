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

    private final int totalNumberOfNodes;
    volatile boolean electionStarted = false;
    volatile boolean leaderReady = false;
    private ScheduledFuture<?> heartbeatTask;
    final LeaderCoordinator coordinator = new LeaderCoordinator(this);
    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    final LogStore log;

    private static final long TIME_BETWEEN_HEARTBEAT = 10;
    private static final long HEARTBEAT_TIMEOUT_BEFORE_ELECTION = 15_000;
    private static final long ELECTION_DURATION = 2;
    static final int NB_NODES = 3;
    static final int QUORUM = NB_NODES / 2 + 1;

    public NodeImpl(int nodeId, int totalNumberOfNodes) throws IOException {
        super();
        this.nodeId = nodeId;
        this.lastHeartbeat = System.currentTimeMillis();
        this.log = new LogStore(nodeId);
        this.totalNumberOfNodes = totalNumberOfNodes;
    }

    synchronized int getEpoch() {
        return epoch;
    }

    boolean isLeader() {
        return leaderReady && leaderId == nodeId;
    }

    /**
     * Permet de retrouver un noeud grâce à son id.
     * Permet ensuite d'appeler des méthodes à distance sur un pair.
     * @param id id du noeud que l'on veut renvoyer.
     * @return le stub RMI correspondant à l'id.
     * @throws Exception
     */
    NodeServer getPeer(int id) throws Exception {
        return (NodeServer) LocateRegistry.getRegistry("localhost", 5000 + id).lookup("Node" + id);
    }

    /**
     * Démarre le fonctionnement du noeud, le noeud 1 est initialement le leader.
     * Si le serveur est un leader, il envoie des heartbeat toutes les 10 secondes aux autres serveurs.
     * Sinon, le follower vérifie toutes les secondes de quand date le dernier heartbeat.
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
     * Méthode pour le leader.
     * Envoie un hearbeat à chaque noeud du réseau pour leur signifier qu'il est vivant et communiquer son epoch et son id.
     * Affiche aussi si le heartbeat est reçu ou non.
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
     * Fonction pour les followers.
     * Leader envoie une proposition d'opération et chaque follower répond s'il accepte de la logger.
     * Créer une entrée de log avec le statut proposed si la proposition ne vient pas d'un leader perimé.
     * @param proposeEpoch epoch du leader qui émet la proposiion
     * @param proposeSeq numéro de séquence de la proposition dans l'epoch du leader
     * @param op opération à journaliser (WRITE ou GUESS)
     * @param value valeur associé à l'opération
     * @return 1 si la proposition a été journaliser (équivalent de ACK), 0 sinon
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
     * Pour les followers.
     * LogStore recherche l'entrée identifié par le couple (epoch, seq) et fait passer son statut de PROPOSED à COMMITTED.
     * @param commitEpoch epoch de la proposition
     * @param commitSeq numéro de séquence de la proposition
     * @return 1 si le commit a été effectué, 0 si échec.
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
     * Récupère la valeur de la dernière opération validée dans le journal.
     * @return la valeur de la dernière entrée validée, -1 sinon.
     */
    @Override
    public int getTarget() {
        LogEntry lastCommitedEntry = log.getLastCommittedEntry();
        return lastCommitedEntry == null ? -1 : lastCommitedEntry.value;
    }

    /**
     * Méthode pour les followers.
     * Refuse les heartbeats provenant d'une epoch plus ancienne ou si le noeud est leader et reçoit un heartbeat d'un noeud d'id supérieur.
     * S'il est accepté, met à jour le leader connu, l'epoch, la date de récéption et l'état de l'élection.
     * Le noeud arrête également sa tâche d'envoi de heartbeats.
     * @param heartbeatEpoch l'epoch du heartbeat envoyé par le leader.
     * @param from : id du leader
     * @return true si le heartbeat est accepté, false sinon
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
     * Méthode pour les followers.
     * Vérifie si le leader envoie toujours des heartbeats.
     * Si le dernier heartbeat date de plus de HEARTBEAT_TIMEOUT_BEFORE_ELECTION secondes, déclenche une élection si une n'est pas déjà en cours.
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
     * Démarre une élection.
     * Le noeud se déclare initialement comme le meilleur candidat, mémorise son état, diffuse sa candidature puis planifie la fin.
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
     * Compare un candidat avec le meilleur candidat actuellement connu.
     * L'ordre de priorité est :
     * 1. L'epoch la plus élevée.
     * 2. Le numéro de séquence le plus élevé si les epochs sont égales.
     * 3. L'identifiant le plus petit si les deux valeurs précendentes sont égales.
     * @param candidateEpoch epoch du candidat reçu.
     * @param candidateSeq numéro de séquence du candidat reçu.
     * @param candidateId identifiant du candidat reçu.
     * @return true si le candidat est meilleur que le candidat actuel.
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
     * Pour les followers.
     * Reçoit une candidature provenant d'une autre noeud.
     * Si ce noeud ne participe pas encore à une élection, il la rejoint, se déclare candidat et programme la fin de celle-ci.
     * Il compare ensuite le candidat reçu avec le meilleur candidat connu.
     * @param candidateEpoch epoch annoncé par le candidat.
     * @param candidateSeq numéro de séquence annonce par le candidat.
     * @param from identifiant du noeud candidat.
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
     * Termine l'élection et détermine si ce noeud devient leader.
     * Si le noeud est le meilleur candidat connu, il devient leader.
     * Sinon, il reconnait le meilleur candidat comme leader.
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
     * Fait passer ce noeud au rôle leader.
     * Augmente l'epoch de 1 et réinitialise la séquence à 0.
     * Envoie également ses logs à tous ses pais et attend un quorum d'ACK.
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
     * Le leader appelle cette méthode sur chaque follower pour leur imposer son log.
     * Remplace le log local par celui du leader, marque tout COMMITED, adopter l'epoch et le leader.
     * @param newEpoch nouvelle epoch
     * @param from Id du nouveau leader
     * @param leaderLog Log du leader
     * @return true si pas d'erreur, false si le leader est perimé.
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
     * Diffuse la candidature de ce noeud à tous les autres noeuds.
     * Chaque noeud reçoit l'epoch, le numéro de séquence et l'identifiant du candidat afin de comparer les candidatures.
     * @param candidateEpoch epoch du candidat
     * @param candidateSequence numéro de séquence du candidat
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
     * Si le noeud est leader, il traite la requete.
     * Sinon, il la relaie au leader.
     * @param op opération reçue par le noeud
     * @param value valeur reçue par le noeud
     * @param fromClient client d'origine.
     * @return si write, la valeur commitée, pour guess, le numéro de round et -1 so refus.
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
