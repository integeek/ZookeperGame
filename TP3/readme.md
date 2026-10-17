# TP 3 : Fault tolerance, Leader election, ZooKeeper-like System

## Contenu des fichiers

| File | Role |
|---|---|
| `NodeServer.java` | RMI interface shared by all nodes and clients |
| `NodeImpl.java` | Node logic: heartbeat, election, log synchronisation, forwarding |
| `LeaderCoordinator.java` | Leader role: assigns `(epoch, seq)`, sends proposals, collects ACKs, broadcasts COMMIT, runs the game rounds |
| `LogStore.java` | Persistent log (one file per node: `log-node<ID>.txt`) |
| `LogEntry.java`, `Op.java`, `Status.java` | Log entry format (`epoch;seq;op;value;status`) |
| `NodeMain.java` | Starts one node |
| `ClientApp.java` | Command-line client |

## Requirements

- Java 21 or later
- All processes run on `localhost`, one terminal per process
- Node `N` listens on port `5000 + N` (nodes 1 to 5 use ports 5001 to 5005)

## Compilation

From the folder containing the sources, run this command :

```
javac *.java
```

## Starting the nodes
To start the nodes, you must specify the node number as well as the total number of nodes in the network, as follows. You need to open one terminal for each node.

```
java NodeMain <nodeId> <totalNumberOfNodes>
```
So, for example, for node 1, assuming there are 5 nodes:
```
java NodeMain 1 5
```
Node 1 is the initial leader (epoch 1). The other nodes are followers and trigger an election if they stop receiving heartbeats.

Each node stores its log in `log-node<ID>.txt` in the working directory. Delete these files between two test sessions, otherwise the nodes reload the previous log:

```
rm log-node*.txt
```

Keep the files if you want to demonstrate that a log survives a restart.

## Starting the clients

To start the clients, you must specify the node with which each client will communicate, as well as its ID, in the following manner. You must launch a separate terminal for each client.
```
java ClientApp <node> <clientId>
```

So, for example, if we want our client 1 to target node 3:
```
java ClientApp 3 1
```

The first line printed is `Connected to Node X`.

### Client commands

| Command | Effect |
|---|---|
| `write <0-100>` | Sets the initial target. Only accepted when no target exists yet |
| `guess <0-100>` | Submits a guess for the current round |
| `target` | The target is hidden, so this only prints a reminder |
| `quit` | Exits the client |

## Simulate a leader crash
To simulate a leader crash, simply start the nodes as follows. For instance, if you want 3 nodes: 
```
java NodeMain 1 3
java NodeMain 2 3
java NodeMain 3 3
```
Since node 1 is the leader, pressing Ctrl+C in its terminal will cause the leader to crash, and elections will begin.