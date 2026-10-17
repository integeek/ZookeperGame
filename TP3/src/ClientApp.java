import java.rmi.registry.LocateRegistry;
import java.util.Scanner;

public class ClientApp {
    interface Call<T> { T run(NodeServer n) throws Exception; }

    private static NodeServer node;
    private static int preferred;
    private static int myTargetRound = -1;

    private static NodeServer connect() {
        for (int i = 0; i < NodeImpl.NB_NODES; i++) {
            int id = (preferred - 1 + i) % NodeImpl.NB_NODES + 1;
            try {
                NodeServer n = (NodeServer) LocateRegistry.getRegistry("localhost", 5000 + id).lookup("Node" + id);
                n.getTarget();
                System.out.println("Connected to Node " + id);
                return n;
            } catch (Exception ignored) {}
        }
        return null;
    }

    /** Exécute l'appel, se reconnecte à un autre nœud en cas de panne. null si tout échoue. */
    private static synchronized <T> T call(Call<T> c) {
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                if (node == null) {
                    node = connect();
                }
                if (node == null) {
                    return null;
                }
                return c.run(node);
            } catch (Exception e) {
                node = null;
            }
        }
        return null;
    }

    public static void main(String[] args) {
        preferred = Integer.parseInt(args[0]);
        int clientId = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        call(NodeServer::getTarget);
        System.out.println("Type write <0-100> to set the target (if none exists), or guess <0-100> to play");

        Thread poller = new Thread(() -> {
            int lastRound = -1;
            while (true) {
                Integer round = call(n -> n.getRound());
                if (round != null && round != lastRound) {
                    if (round > 1 && lastRound != -1) {
                        System.out.println("> Round " + round + " : a new hidden target has been chosen, guess it");
                    }
                    lastRound = round;
                }
                try { Thread.sleep(1000); } catch (InterruptedException e) { return; }
            }
        });
        poller.setDaemon(true);
        poller.start();

        System.out.println("Commands : write <x> | guess <x> | target | quit");
        Scanner sc = new Scanner(System.in);
        while (sc.hasNextLine()) {
            String[] t = sc.nextLine().trim().split("\\s+");
            try {
                switch (t[0]) {
                    case "write" -> {
                        Integer cur = call(NodeServer::getTarget);
                        if (cur == null || cur != -1) {
                            System.out.println("A target already exists, use guess");
                            break;
                        }
                        Integer r = call(n -> n.forward(Op.WRITE, Integer.parseInt(t[1]), clientId));
                        if (r == null || r == -1) {
                            System.out.println("Writing rejected or failed");
                        } else {
                            Integer rd = call(NodeServer::getRound);
                            myTargetRound = (rd == null) ? 1 : rd;   // après le write, le round vaut 1
                            System.out.println("Target set at " + r + ", other clients can guess");
                        }
                    }
                    case "guess" -> {
                        Integer curRound = call(NodeServer::getRound);
                        if (curRound != null && curRound == myTargetRound) {
                            System.out.println("You set this target, wait for the next round to guess");
                            break;
                        }
                        Integer cur = call(NodeServer::getTarget);
                        if (cur == null || cur == -1) {
                            System.out.println("No target yet, wait for the first client");
                            break;
                        }
                        Integer r = call(n -> n.forward(Op.GUESS, Integer.parseInt(t[1]), clientId));
                        System.out.println(r == null || r == -1 ? "Guess rejected" : "Guess recorded (round " + r + ")");
                    }
                    case "target" -> System.out.println("The target is hidden");
                    case "quit" -> { return; }
                    default -> System.out.println("?");
                }
            } catch (Exception e) {
                System.out.println("Invalid command");
            }
        }
    }}