import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.List;

public class MainNode {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java MainNode <nodeId> <port>");
            return;
        }

        int nodeId = Integer.parseInt(args[0]);
        int port = Integer.parseInt(args[1]);
        int totalNodes = 5;

        try {
            LogStore logStore = new LogStore(nodeId);
            NodeServerImpl nodeServer = new NodeServerImpl(nodeId, logStore);
            
            LocateRegistry.createRegistry(port);
            Naming.rebind("rmi://localhost:" + port + "/NodeServer", nodeServer);
            System.out.println("Node " + nodeId + " started on port " + port);

            // FIX: Store URLs as strings for dynamic lookup later, avoiding crashes if a node is offline
            List<String> peerUrls = new ArrayList<>();
            for (int i = 1; i <= totalNodes; i++) {
                if (i != nodeId) {
                    peerUrls.add("rmi://localhost:" + (5000 + i) + "/NodeServer");
                }
            }
            
            nodeServer.setPeers(peerUrls);

            ElectionManager electionManager = new ElectionManager(nodeId, logStore, peerUrls, () -> {
                LogStore.LogEntry lastEntry = logStore.getLastLogEntry();
                int newEpoch = (lastEntry != null ? lastEntry.epoch : 0) + 1;
                
                LeaderCoordinator coordinator = new LeaderCoordinator(nodeId, logStore, peerUrls, totalNodes, newEpoch);
                nodeServer.setModules(coordinator, null); 
                coordinator.startHeartbeatBroadcast();
            });

            nodeServer.setModules(null, electionManager);
            electionManager.startHeartbeatMonitor();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}