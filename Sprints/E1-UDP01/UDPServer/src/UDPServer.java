import java.net.*;
import java.io.*;

public class UDPServer {

    public static void main(String args[]) {
        DatagramSocket aSocket = null;

        try {
            aSocket = new DatagramSocket(6789);
            byte[] buffer = new byte[1000];

            while (true) {
                DatagramPacket request = new DatagramPacket(buffer, buffer.length);
                aSocket.receive(request);
                String receivedMessage = new String(request.getData(),0,request.getLength());

                int commaIndex = receivedMessage.indexOf(',');
                String message = "";
                int sequenceNumber = 0;

                if (commaIndex != -1)
                {
                    String number = receivedMessage.substring(0, commaIndex);
                    message = receivedMessage.substring(commaIndex+1);

                    try{
                        sequenceNumber = Integer.parseInt(number);

                    }catch(NumberFormatException e){
                        System.out.println("Invalid number");
                        continue;
                    }
                }
                else {
                    System.out.println("Invalid format");
                    continue;
                }

                DatagramPacket reply = new DatagramPacket(request.getData(),
                        request.getLength(), request.getAddress(), request.getPort());

                aSocket.send(reply);
            }
        } catch (SocketException e) { System.out.println("Socket: " + e.getMessage());
        } catch (IOException e)     { System.out.println("IO: " + e.getMessage());
        } finally { if (aSocket != null) aSocket.close(); }
    }
}