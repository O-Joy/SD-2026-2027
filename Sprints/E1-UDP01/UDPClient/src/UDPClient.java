import java.net.*;
import java.io.*;
import java.util.Scanner;

public class UDPClient {

    public static void main(String args[]) {
        DatagramSocket aSocket = null;

        try {
            aSocket = new DatagramSocket();

            InetAddress serverHost = InetAddress.getByName("localhost");
            int serverPort = 6789;

            Scanner scanner = new Scanner(System.in);

            int sequenceNumber = 0;

            System.out.println("Cliente UDP iniciado. Escreva uma mensagem e prime Enter para enviar.");
            System.out.println("Escreva 'sair' para terminar.");

            while (true) {
                System.out.print("> ");
                String userInput = scanner.nextLine();

                if (userInput.equalsIgnoreCase("sair")) {
                    break;
                }

                String messageToSend = sequenceNumber + "," + userInput;
                byte[] data = messageToSend.getBytes();

                DatagramPacket request = new DatagramPacket(data, data.length, serverHost, serverPort);
                aSocket.send(request);

                byte[] buffer = new byte[1000];
                DatagramPacket reply = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(reply);

                String receivedReply = new String(reply.getData(), 0, reply.getLength());
                System.out.println("Servidor respondeu: " + receivedReply);

                sequenceNumber++;
            }

            System.out.println("Cliente terminado.");

        } catch (SocketException e) {
            System.out.println("Socket: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            if (aSocket != null) aSocket.close();
        }
    }
}