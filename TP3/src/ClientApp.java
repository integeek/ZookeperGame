import java.rmi.registry.LocateRegistry;
import java.util.Scanner;

public class ClientApp {
    interface NodeOperation {
        int run(NodeServer n) throws Exception;
    }

    private static NodeServer connectedNode;
    private static int preferredNodeId;
    private static int myTargetRound = -1;
    static final int NB_NODES = 3;

    /**
     * Searches for a cluster node that responds, starting with the preferred node and then trying the others.
     * @return the node to which one is connected, null otherwise
     */
    private static NodeServer connectToAvailableNode() {
        for (int offset = 0; offset < NB_NODES; offset++) {
            int id = (preferredNodeId - 1 + offset) % NB_NODES + 1;
            try {
                NodeServer candidateNode = (NodeServer) LocateRegistry.getRegistry("localhost", 5000 + id).lookup("Node" + id);
                System.out.println("Connected to Node " + id);
                return candidateNode;
            } catch (Exception ignored) {}
        }
        return null;
    }

    /**
     * Performs an operation on the connected node, with automatic reconnection in the event of a failure.
     * @param nodeOperation operation to be performed on the node
     * @return the result of the operation, -1 if there is a problem
     */
    private static synchronized int executeWithRetry(NodeOperation nodeOperation) {
        for (int attemptNumber = 0; attemptNumber < 2; attemptNumber++) {
            try {
                if (connectedNode == null) {
                    connectedNode = connectToAvailableNode();
                }
                if (connectedNode == null) {
                    return -1;
                }
                return nodeOperation.run(connectedNode);
            } catch (Exception e) {
                connectedNode = null;
            }
        }
        return -1;
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("java ClientApp <node> <clientId>");
            return;
        }
        preferredNodeId = Integer.parseInt(args[0]);

        int clientId = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        executeWithRetry(NodeServer::getTarget);
        System.out.println("Type write <0-100> to set the target (if none exists), or guess <0-100> to play");

        getThread().start();

        System.out.println("Commands : write <x> | guess <x> | quit");
        Scanner sc = new Scanner(System.in);
        while (sc.hasNextLine()) {
            String[] commandParts = sc.nextLine().trim().split("\\s+");
            try {
                switch (commandParts[0]) {
                    case "write" -> {
                        int existingTarget = executeWithRetry(NodeServer::getTarget);
                        if (existingTarget != -1) {
                            System.out.println("A target already exists, use guess");
                            break;
                        }
                        int operationResult = executeWithRetry(n -> n.forward(Op.WRITE, Integer.parseInt(commandParts[1]), clientId));
                        if (operationResult == -1) {
                            System.out.println("Writing rejected or failed");
                        } else {
                            myTargetRound = executeWithRetry(NodeServer::getRound);

                            System.out.println("Target set at " + operationResult + ", other clients can guess");
                        }
                    }
                    case "guess" -> {
                        int currentRound = executeWithRetry(NodeServer::getRound);
                        if (currentRound == myTargetRound) {
                            System.out.println("You set this target, wait for the next round to guess");
                            break;
                        }
                        int currentTarget = executeWithRetry(NodeServer::getTarget);
                        if (currentTarget == -1) {
                            System.out.println("No target yet, wait for the first client");
                            break;
                        }
                        int r = executeWithRetry(n -> n.forward(Op.GUESS, Integer.parseInt(commandParts[1]), clientId));
                        System.out.println(r == -1 ? "Guess rejected" : "Guess recorded (round " + r + ")");
                    }
                    case "quit" -> {
                        return;
                    }
                    default -> System.out.println("?");
                }
            } catch (Exception e) {
                System.out.println("Invalid command");
            }
        }
    }

    /**
     * Creates a monitoring thread.
     * Every second, it requests the round number from the cluster and notifies the player when a new target has been chosen.
     * @return the round-monitoring thread.
     */
    private static Thread getThread() {
        Thread poller = new Thread(() -> {
            int lastRound = -1;
            while (true) {
                int currentRound = executeWithRetry(NodeServer::getRound);
                if (currentRound != lastRound) {
                    if (currentRound > 1 && lastRound != -1) {
                        System.out.println("> Round " + currentRound + " : a new hidden target has been chosen, guess it");
                    }
                    lastRound = currentRound;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    return; }
            }
        });
        poller.setDaemon(true);
        return poller;
    }
}