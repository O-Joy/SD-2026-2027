package tcp01;
import java.io.*;
import java.net.*;

public class TCPClient {
    public static void main(String[] args) {
        Socket s = null;
        try {
            int serverPort = 7896;                              
            s = new Socket("localhost", serverPort);
            
            ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
            DataInputStream in = new DataInputStream(s.getInputStream());
            
            // Criar a pessoa e enviar
            Person p = new Person("Joao Silva", 1995);
            out.writeObject(p); // Envia o objeto inteiro serializado!                   
            
            // O cliente continua a receber a resposta como texto
            String data = in.readUTF();                         
            System.out.println("Servidor respondeu: " + data);
            
        } catch (UnknownHostException e) {
            System.out.println("Sock: " + e.getMessage());
        } catch (EOFException e) {
            System.out.println("EOF: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            if (s != null) {
                try { s.close(); } catch (IOException e) { System.out.println("close: " + e.getMessage()); }
            }
        }
    }
}