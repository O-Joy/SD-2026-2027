package tcp01;
import java.io.*;
import java.net.*;

public class Connection extends Thread {
    ObjectInputStream in;
    DataOutputStream out;
    Socket clientSocket;

    public Connection(Socket aClientSocket) {
        try {
            clientSocket = aClientSocket;
            in = new ObjectInputStream(clientSocket.getInputStream());
            out = new DataOutputStream(clientSocket.getOutputStream());
            this.start();
        } catch (IOException e) {
            System.out.println("Connection: " + e.getMessage());
        }
    }

    @Override
    public void run() {
        try {
            // readObject reconstrói a Person E o Place que ela referencia
            Person p = (Person) in.readObject();

            // O servidor devolve agora a localidade, que só existe no Place
            out.writeUTF(p.getPlace().getLocality());

        } catch (ClassNotFoundException e) {
            System.out.println("Classe nao encontrada: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            try { clientSocket.close(); } catch (IOException e) { /* falha ao fechar */ }
        }
    }
}
