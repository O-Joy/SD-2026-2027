import java.net.*;
import java.io.*;

public class UDPServer {

    public static void main(String args[]) {
        DatagramSocket aSocket = null;
        
        // Estado inicial do servidor: L guarda o numero da ultima mensagem aceite em ordem
        int L = 0; 

        try {
            aSocket = new DatagramSocket(6789);
            System.out.println("Servidor UDP ativo no porto 6789. Estado inicial: L = " + L);
            byte[] buffer = new byte[1000];

            while (true) {
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(request);

                String receivedMessage = new String(request.getData(), 0, request.getLength()).trim();
                String replyMessage;

                int commaIndex = receivedMessage.indexOf(',');

                // Validacao de integridade e formato
                if (commaIndex != -1) {
                    String numberStr = receivedMessage.substring(0, commaIndex).trim();

                    try {
                        int sequenceNumber = Integer.parseInt(numberStr);

                        // Regra de decisao do protocolo
                        if (sequenceNumber == L + 1) {
                            // Mensagem em ordem: aceita e atualiza o estado L
                            L = sequenceNumber;
                            replyMessage = receivedMessage; // Comportamento de echo
                            System.out.println("[ACEITE] Mensagem " + sequenceNumber + " em ordem. Novo L = " + L);
                        } else {
                            // Mensagem fora de ordem: rejeita, nao altera L e pede a esperada
                            replyMessage = "waitingfor," + (L + 1);
                            System.out.println("[FORA DE ORDEM] Recebido " + sequenceNumber + ", esperado " + (L + 1) + ". L mantem-se " + L);
                        }

                    } catch (NumberFormatException e) {
                        System.out.println("[FORMATO INVALIDO] Numero nao reconhecido: " + receivedMessage);
                        replyMessage = "waitingfor," + (L + 1);
                    }
                } else {
                    System.out.println("[FORMATO INVALIDO] Mensagem sem virgula: " + receivedMessage);
                    replyMessage = "waitingfor," + (L + 1);
                }

                // Envio da resposta correspondente
                byte[] replyData = replyMessage.getBytes();
                DatagramPacket reply = new DatagramPacket(
                    replyData, 
                    replyData.length, 
                    request.getAddress(), 
                    request.getPort()
                );

                aSocket.send(reply);
            }
        } catch (SocketException e) { 
            System.out.println("Socket: " + e.getMessage());
        } catch (IOException e) { 
            System.out.println("IO: " + e.getMessage());
        } finally { 
            if (aSocket != null) aSocket.close(); 
        }
    }
}