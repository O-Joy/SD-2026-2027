package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;

// Ponto 2 — código do enunciado. Os comentários [BLOQUEIA] respondem ao "Antes de avançar" do ponto 3.
public class MulticastLeader {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        try (MulticastSocket socket = new MulticastSocket();                // porto efémero: o líder só envia, não se junta ao grupo
             Scanner sc = new Scanner(System.in)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.setTimeToLive(1);                                // o datagrama não sai da rede local
            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq();                              // continua a numeração após reinício
            // [BLOQUEIA brevemente] leitura do disco: espera pelo SO a ler lider.txt
            System.out.println("Líder pronto. Formato: <sensor> <temperatura> | 'sair' para terminar");
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;                       // bloqueia à espera do utilizador
                // [BLOQUEIA] espera indefinidamente que o utilizador escreva uma linha
                //            (ou que o stdin feche — EOF, Ctrl+D/Ctrl+Z)
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;
                String[] p = line.split("\\s+");
                if (p.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }
                double temp;
                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + p[1]);
                    continue;
                }
                SensorRecord r = SensorRecord.now(++seq, p[0], temp);
                file.append(r);                                     // 1) escreve localmente
                // [BLOQUEIA brevemente] escrita em disco: espera que o SO aceite a escrita
                byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);
                socket.send(new DatagramPacket(m, m.length, group, PORT));  // 2) propaga para o grupo
                // [NÃO espera pelas réplicas] só espera que o SO copie o datagrama para o
                //   buffer de envio; retorna mesmo que não exista nenhuma réplica no grupo
                System.out.println("Registado e enviado: " + r.toLine());
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}
