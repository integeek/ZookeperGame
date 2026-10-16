import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class NodeMain {
    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            System.out.println("Problème");
            return;
        }
        int nodeId = Integer.parseInt(args[0]);
        int port = 5000 + nodeId;

        Registry registry = LocateRegistry.createRegistry(port);
        NodeImpl node = new NodeImpl(nodeId);
        registry.rebind("Node" + nodeId, node);

        System.out.println("Node " + nodeId + " started on port " + port);
        node.start();
    }
}
