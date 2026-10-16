import java.rmi.registry.LocateRegistry;
import java.util.Scanner;

public class ClientApp {
    public static void main(String[] args) throws Exception {
        int nodeId = Integer.parseInt(args[0]);
        int clientId = args.length > 1 ? Integer.parseInt(args[1]) : 1;
        NodeServer node = (NodeServer) LocateRegistry.getRegistry("localhost", 5000 + nodeId).lookup("Node" + nodeId);

        System.out.println("Commandes : write <x> | guess <x> | target | quit");
        Scanner sc = new Scanner(System.in);
        while (sc.hasNextLine()) {
            String[] t = sc.nextLine().trim().split("\\s+");
            try {
                switch (t[0]) {
                    case "write"  -> System.out.println("result = " + node.forward(Op.WRITE, Integer.parseInt(t[1]), clientId));
                    case "guess"  -> System.out.println("result = " + node.forward(Op.GUESS, Integer.parseInt(t[1]), clientId));
                    case "target" -> System.out.println("target = " + node.getTarget());
                    case "quit"   -> { return; }
                    default       -> System.out.println("?");
                }
            } catch (Exception e) {
                System.out.println("Erreur : " + e.getMessage());
            }
        }
    }
}