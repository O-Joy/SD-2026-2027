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

            int sequenceNumber = 1;

            System.out.println("Cliente UDP iniciado. Escreva uma mensagem e prime Enter para enviar.");
            System.out.println("Escreva 'sair' para terminar.");

            while (true) {
                System.out.print("Mensagem (ou 'sair'): ");
                String userInput = scanner.nextLine();

                if (userInput.equalsIgnoreCase("sair")) {
                    break;
                }

                String mode;
                do {
                    System.out.print("Modo automático ou manual? (A/M): ");
                    mode = scanner.nextLine().trim().toUpperCase();

                    if (!mode.equals("A") && !mode.equals("M")) {
                        System.out.println("Modo inválido. Escolha A ou M.");
                    }
                } while (!mode.equals("A") && !mode.equals("M"));

                int messageNumber;

                if (mode.equals("A")) {
                    messageNumber = sequenceNumber;
                    sequenceNumber++;
                } else {
                    Integer parsed = null;
                    do {
                        System.out.print("Número da mensagem: ");
                        String text = scanner.nextLine().trim();

                        try {
                            parsed = Integer.parseInt(text);
                        } catch (NumberFormatException e) {
                            System.out.println("Número inválido. Escreva um inteiro.");
                        }
                    } while (parsed == null);

                    messageNumber = parsed;
                }

                String messageToSend = messageNumber + "," + userInput;

                byte[] data = messageToSend.getBytes();

                DatagramPacket request = new DatagramPacket(data, data.length, serverHost, serverPort);
                aSocket.send(request);

                byte[] buffer = new byte[1000];
                DatagramPacket reply = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(reply);

                String receivedReply = new String(reply.getData(), 0, reply.getLength());

                if (receivedReply.startsWith("waitingfor,")) {
                    System.out.println("Servidor pede: " + receivedReply);
                } else {
                    System.out.println("Echo recebido: " + receivedReply);
                }

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