import java.rmi.Naming;
import java.util.Scanner;
import java.util.UUID;

public class ClientApp {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Usage: java ClientApp <port_du_noeud>");
            return;
        }

        try {
            int port = Integer.parseInt(args[0]);
            String clientId = "Client-" + UUID.randomUUID().toString().substring(0, 5);
            String registryUrl = "rmi://localhost:" + port + "/NodeServer";
            INodeServer nodeServer = (INodeServer) Naming.lookup(registryUrl);

            System.out.println("=== Connecté au système Zookeeper-like (" + clientId + ") ===");

            try (Scanner scanner = new Scanner(System.in)) {
                while (true) {
                    System.out.print("> ");

                    if (!scanner.hasNextLine()) {
                        break;
                    }

                    String input = scanner.nextLine().trim();
                    if (input.isEmpty()) {
                        continue;
                    }

                    String[] parts = input.split("\\s+");
                    String command = parts[0].toLowerCase();

                    if ("exit".equals(command)) {
                        break;
                    }

                    try {
                        if (("write".equals(command) || "guess".equals(command))
                                && parts.length == 2) {
                            int value = Integer.parseInt(parts[1]);
                            String type = "write".equals(command) ? "WRITE" : "GUESS";

                            nodeServer.forwardRequest(type, value, clientId);
                            System.out.println("Requête " + type + "(" + value + ") envoyée.");
                        } else if ("read".equals(command) && parts.length == 1) {
                            System.out.println("La cible actuelle est : " + nodeServer.getTarget());
                        } else {
                            System.out.println("Commande invalide.");
                        }
                    } catch (NumberFormatException e) {
                        System.out.println("La valeur doit être un nombre.");
                    }
                }
            }
        } catch (NumberFormatException e) {
            System.err.println("Le port doit être un nombre.");
        } catch (Exception e) {
            System.err.println("Erreur de connexion du client : " + e.getMessage());
        }
    }
}