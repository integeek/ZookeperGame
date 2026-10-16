import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
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

    private static final long TIME_BETWEEN_HEARTBEAT = 10;
    private static final long HEARTBEAT_TIMEOUT_BEFORE_ELECTION = 15_000;
    private static final long ELECTION_DURATION = 2;
    volatile boolean electionStarted = false;
    private ScheduledFuture<?> heartbeatTask;

    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    public NodeImpl(int nodeId) throws RemoteException {
        super();
        this.nodeId = nodeId;
        this.lastHeartbeat = System.currentTimeMillis();
    }

    /**
     * Permet de retrouver un noeud grâce à son id.
     * @param id id du noeud que l'on veut renvoyer.
     * @return le noeud correspondant à l'id.
     * @throws Exception
     */
    NodeServer getPeer(int id) throws Exception {
        return (NodeServer) LocateRegistry.getRegistry("localhost", 5000 + id).lookup("Node"+id);
    }

    /**
     * Démarre le fonctionnement du noeud, le noeud 1 est initialement le leader.
     * Si le serveur est un leader, il envoie des heartbeat toutes les 30 secondes aux autres serveurs.
     * Sinon, le follower vérifie toutes les secondes de quand date le dernier heartbeat.
     */
    public void start() {
        if (nodeId == 1) {
            leaderId = nodeId;

            System.out.println("Node 1 is the leader");
            scheduler.scheduleAtFixedRate(this::sendHeartBeats, 0, TIME_BETWEEN_HEARTBEAT, TimeUnit.SECONDS);
        } else {
            scheduler.scheduleAtFixedRate(this::checkHeartbeat, 1, 1, TimeUnit.SECONDS);
        }
    }

    /**
     * Méthode pour le leader.
     * Pour chaque serveur du réseau, il vérifie si le heartbeat est reçu ou non.
     */
    void sendHeartBeats() {
        for (int id = 1; id <= 3; id++) {
            if (id == nodeId) {
                continue;
            }
            try {
                boolean received = getPeer(id).heartbeat(epoch, nodeId);
                System.out.println("Heartbeat sent to Node " + id + " | accepted = " + received);
            } catch (Exception e) {
                System.out.println("Cannot contact Node " + id);
            }
        }
    }

    @Override
    public int propose(int epoch, int seq, int op, int value) throws RemoteException {
        return 0;
    }

    @Override
    public boolean ack(int epoch, int seq, int from) throws RemoteException {
        return false;
    }

    @Override
    public int commit(int epoch, int seq) throws RemoteException {
        return 0;
    }

    /**
     * Méthode pour les followers.
     * Refuse les heartbeats provenant d'une epoch plus ancienne.
     * S'il est accepté, met à jour le leader connu, l'epoch, la date de récéption et l'état de l'élection.
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

        long elasped = System.currentTimeMillis() - lastHeartbeat;
        if (elasped > HEARTBEAT_TIMEOUT_BEFORE_ELECTION && !electionStarted) {
            System.out.println("Node " + nodeId + " : no heartbeat received for " + HEARTBEAT_TIMEOUT_BEFORE_ELECTION + "  seconds");
            startElection();
        }
    }

    /**
     * Démarre une élection.
     * Le noeud se déclare initialement candidat, mémorise son état, diffuse sa candidature puis attend la fin.
     */
    void startElection() {
        synchronized (this) {
            if (electionStarted) {
                return;
            }
            electionStarted = true;
            bestCandidateEpoch = epoch;
            bestCandidateSeq = seq;
            bestCandidateId = nodeId;
        }
        int e, s;
        synchronized (this) {
            e = epoch;
            s = seq;
        }

        broadcastCandidacy(e, s);
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
            final int e = epoch, s = seq;
            scheduler.execute(() -> broadcastCandidacy(e, s));
        }
    }

    /**
     * Termine l'élection et détermine si ce noeud devient leader.
     * Si le noeud est le meilleur candidat connu, il devient leader.
     * Sinon, il reconnait le meilleur candidat comme leader.
     */
    void finishElection() {
        boolean iWon;
        synchronized (this) {
            System.out.println("Node " + nodeId + " election result: Node " + bestCandidateId);
            iWon = (bestCandidateId == nodeId);
            if (!iWon) {
                leaderId = bestCandidateId;
                lastHeartbeat = System.currentTimeMillis();
                electionStarted = false;
            }
        }
        if (iWon) {
            becomeLeader();
        } else {
            System.out.println("Node " + nodeId + " recognizes Node " + leaderId + " as leader");
        }
    }

    /**
     * Fait passer ce noeud au rôle leader.
     */
    void becomeLeader() {
        synchronized (this) {
            epoch++;
            seq = 0;
            leaderId = nodeId;
            electionStarted = false;
        }
        sendHeartBeats();
        heartbeatTask = scheduler.scheduleAtFixedRate(this::sendHeartBeats, TIME_BETWEEN_HEARTBEAT, TIME_BETWEEN_HEARTBEAT, TimeUnit.SECONDS);
    }

    /**
     * Diffuse la candidature de ce noeud à tous les autres noeuds.
     * Chaque noeud reçoit l'epoch, le numéro de séquence et l'identifiant du candidat afin de comparer les candidatures.
     * @param e epoch du candidat
     * @param s numéro de séquence du candidat
     */
    void broadcastCandidacy(int e, int s) {
        for (int id = 1; id <= 3; id++) {
            if (id == nodeId) continue;
            try { getPeer(id).election(e, s, nodeId); }
            catch (Exception ex) { System.out.println("Cannot contact Node " + id); }
        }
    }

    @Override
    public void forward(int op, int value, int from_client) throws RemoteException {

    }
}
