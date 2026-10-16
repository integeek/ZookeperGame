import java.rmi.*;

public interface Client extends Remote {

    int write(int number) throws RemoteException;
}
