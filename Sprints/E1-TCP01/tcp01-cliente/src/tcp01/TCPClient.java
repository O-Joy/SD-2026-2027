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

            // Criar o Place e a Person que o referencia
            Place place = new Place("3500-000", "Viseu");
            Person p = new Person("Joao Silva", place, 1995);

            // Escreve-se APENAS a Person: o Place segue com ela automaticamente
            out.writeObject(p);
            out.flush();

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
