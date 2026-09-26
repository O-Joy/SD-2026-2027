import java.net.*;
import java.io.*;
import java.util.Scanner;

public class UDPClient {

    public static void main(String args[]) {
        DatagramSocket aSocket = null;
        Scanner scanner = new Scanner(System.in);

        try {
            aSocket = new DatagramSocket();
            InetAddress serverHost = InetAddress.getByName("localhost");
            int serverPort = 6789;

            System.out.println("=== Cliente UDP Iniciado ===");
            System.out.println("Escolha o modo de envio:");
            System.out.println("1 - Modo Automatico (1, 2, 3...)");
            System.out.println("2 - Modo Manual (introduz N primeiro, depois a mensagem)");
            System.out.print("Modo [1 ou 2]: ");
            
            int modo = 1;
            try {
                modo = Integer.parseInt(scanner.nextLine().trim());
            } catch (NumberFormatException e) {
                System.out.println("Opcao invalida. A usar Modo Automatico por defeito.");
            }

            int sequenceNumber = 1;

            while (true) {
                String messageToSend;

                if (modo == 2) {
                    // MODO MANUAL: Pergunta primeiro o N
                    System.out.print("\nNumero de sequencia (N) a enviar (ou 'sair' para terminar): ");
                    String seqInput = scanner.nextLine().trim();
                    
                    if (seqInput.equalsIgnoreCase("sair")) {
                        break;
                    }

                    int manualSeq;
                    try {
                        manualSeq = Integer.parseInt(seqInput);
                    } catch (NumberFormatException e) {
                        System.out.println("Numero invalido. Tente novamente.");
                        continue;
                    }

                    // Depois pergunta a mensagem
                    System.out.print("Introduza a mensagem: ");
                    String textInput = scanner.nextLine();
                    
                    // Constroi o pacote final N,mensagem
                    messageToSend = manualSeq + "," + textInput;
                } else {
                    // MODO AUTOMATICO
                    System.out.print("\nIntroduza a mensagem (ou 'sair' para terminar): ");
                    String textInput = scanner.nextLine();
                    
                    if (textInput.equalsIgnoreCase("sair")) {
                        break;
                    }

                    messageToSend = sequenceNumber + "," + textInput;
                    sequenceNumber++;
                }

                // Envio
                byte[] data = messageToSend.getBytes();
                DatagramPacket request = new DatagramPacket(data, data.length, serverHost, serverPort);
                aSocket.send(request);

                // Receção
                byte[] buffer = new byte[1000];
                DatagramPacket reply = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(reply);

                String receivedReply = new String(reply.getData(), 0, reply.getLength());

                // Tratamento visual da resposta
                if (receivedReply.startsWith("waitingfor,")) {
                    String esperado = receivedReply.split(",")[1];
                    System.out.println("<!> AVISO DO SERVIDOR: Mensagem fora de ordem!");
                    System.out.println("    O servidor recebeu, guardou, e esta a espera da mensagem: " + esperado);
                } else if (receivedReply.startsWith("dup,")) {
                    String duplicado = receivedReply.split(",")[1];
                    System.out.println("<!> AVISO: Mensagem " + duplicado + " ja tinha sido processada (duplicado ignorado).");
                } else {
                    System.out.println("[ENTREGUE]: " + receivedReply);
                }
            }

            System.out.println("Cliente terminado.");

        } catch (SocketException e) {
            System.out.println("Socket: " + e.getMessage());
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        } finally {
            if (aSocket != null) aSocket.close();
            scanner.close();
        }
    }
}