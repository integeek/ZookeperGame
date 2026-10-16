import java.rmi.registry.LocateRegistry;
import java.util.Scanner;

public class ClientApp {
    interface Call<T> { T run(NodeServer n) throws Exception; }

    private static NodeServer node;
    private static int preferred;

    private static NodeServer connect() {
        for (int i = 0; i < NodeImpl.NB_NODES; i++) {
            int id = (preferred - 1 + i) % NodeImpl.NB_NODES + 1;
            try {
                NodeServer n = (NodeServer) LocateRegistry.getRegistry("localhost", 5000 + id).lookup("Node" + id);
                n.getTarget();                                   // vérifie que le nœud répond
                System.out.println("[connecté au Node " + id + "]");
                return n;
            } catch (Exception ignored) {}
        }
        return null;
    }

    /** Exécute l'appel, se reconnecte à un autre nœud en cas de panne. null si tout échoue. */
    private static synchronized <T> T call(Call<T> c) {
        for (int attempt = 0; attempt < 2; attempt++) {
            try {
                if (node == null) node = connect();
                if (node == null) return null;
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

        Integer t0 = call(n -> n.getTarget());
        if (t0 != null && t0 == -1) {
            System.out.println("Pas encore de cible : vous êtes le premier client, tapez write <0-100>");
        }

        Thread poller = new Thread(() -> {
            int lastRound = -1;
            while (true) {
                Integer round = call(n -> n.getRound());
                Integer target = call(n -> n.getTarget());
                if (round != null && target != null && round != lastRound) {
                    lastRound = round;
                    System.out.println(">>> Round " + round + " : cible = " + target);
                }
                try { Thread.sleep(1000); } catch (InterruptedException e) { return; }
            }
        });
        poller.setDaemon(true);
        poller.start();

        System.out.println("Commandes : write <x> | guess <x> | target | quit");
        Scanner sc = new Scanner(System.in);
        while (sc.hasNextLine()) {
            String[] t = sc.nextLine().trim().split("\\s+");
            try {
                switch (t[0]) {
                    case "write" -> {
                        Integer r = call(n -> n.forward(Op.WRITE, Integer.parseInt(t[1]), clientId));
                        System.out.println(r == null || r == -1 ? "Écriture refusée ou échouée" : "Cible fixée à " + r);
                    }
                    case "guess" -> {
                        Integer r = call(n -> n.forward(Op.GUESS, Integer.parseInt(t[1]), clientId));
                        System.out.println(r == null || r == -1 ? "Guess refusé" : "Guess enregistré (round " + r + ")");
                    }
                    case "target" -> System.out.println("target = " + call(n -> n.getTarget()));
                    case "quit" -> { return; }
                    default -> System.out.println("?");
                }
            } catch (Exception e) {
                System.out.println("Commande invalide");
            }
        }
    }
}