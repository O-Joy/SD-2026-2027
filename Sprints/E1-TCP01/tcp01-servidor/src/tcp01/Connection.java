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
            // A ordem importa: primeiro abres as streams
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
            // 1. Lemos o objeto (readObject devolve um Object genérico)
            // 2. Fazemos o CAST explícito (Person)
            Person p = (Person) in.readObject();                         
            
            // 3. O servidor responde apenas com o nome da pessoa
            out.writeUTF("Olá, " + p.getName());                                 
            
        } catch (ClassNotFoundException e) {
            // E se o cliente enviar uma classe que o servidor não tem?
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