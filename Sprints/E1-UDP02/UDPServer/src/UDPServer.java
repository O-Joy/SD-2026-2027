import java.net.*;
import java.io.*;
import java.util.*;


public class UDPServer {

static List<String> listaRececao = new ArrayList<>();
static Map<Integer, String> bufferTemporario = new HashMap<>();

    public static int processDeliveredMessages(int nLastMessageInOrder, int nCurrentMessage, String currentMessage) {
        int L = nLastMessageInOrder;

        if (nCurrentMessage == L + 1) {
            listaRececao.add(currentMessage);
            L = nCurrentMessage;

            while (bufferTemporario.containsKey(L + 1)) {
                String proximaMensagem = bufferTemporario.remove(L + 1);
                listaRececao.add(proximaMensagem);
                L = L + 1;
            }

        } else if (nCurrentMessage > L + 1) {
            bufferTemporario.put(nCurrentMessage, currentMessage);
        }

        return L;
    }


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

                        if (sequenceNumber <= L) {
                            // Duplicado ou já processado — L não muda, não passa pelo processDeliveredMessages
                            replyMessage = "dup," + sequenceNumber;
                            System.out.println("[DUPLICADO] Mensagem " + sequenceNumber + " já processada (L=" + L + ")");

                        } else {
                            int oldL = L;
                            L = processDeliveredMessages(L, sequenceNumber, receivedMessage);

                            if (L > oldL) {
                                // Foi entregue
                                replyMessage = receivedMessage;
                                System.out.println("[ACEITE] Mensagem " + sequenceNumber + " entregue. Novo L = " + L
                                        + (L > sequenceNumber ? " (cascata libertou até " + L + ")" : ""));
                            } else {
                                // Ficou retida no buffer temporário, à espera
                                replyMessage = "waitingfor," + (L + 1);
                                System.out.println("[RETIDA] Mensagem " + sequenceNumber + " guardada. A espera de " + (L + 1)
                                        + ". Buffer agora: " + bufferTemporario.keySet());
                            }
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