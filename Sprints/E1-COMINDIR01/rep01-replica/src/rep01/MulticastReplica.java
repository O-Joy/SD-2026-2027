package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

// Ponto 3 — código do enunciado (versão base). Os comentários [BLOQUEIA] e [CONFIA] respondem ao "Antes de avançar".
// O ponto 6 (números de sequência, EM ESPERA, DUPLICADO, REJEITADO) é feito pelo elemento B sobre esta classe.
public class MulticastReplica {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        RecordFile file = new RecordFile("replica-" + id + ".txt");
        try (MulticastSocket socket = new MulticastSocket(PORT)) {   // bind ao porto 6789 (MulticastSocket ativa SO_REUSEADDR,
            //   por isso várias réplicas na mesma máquina partilham o porto)
            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(group);                                // passa a receber o que é enviado ao grupo
            // [não bloqueia] só pede ao SO para aceitar datagramas do grupo
            System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT);
            byte[] buffer = new byte[1000];                         // datagramas maiores do que isto são TRUNCADOS sem aviso
            while (true) {
                DatagramPacket p = new DatagramPacket(buffer, buffer.length);
                socket.receive(p);                                  // bloqueia até chegar um datagrama
                // [BLOQUEIA] espera indefinidamente que chegue um datagrama ao
                //            porto 6789 do grupo (de qualquer emissor, não só do líder)
                String line = new String(p.getData(), 0, p.getLength(), StandardCharsets.UTF_8);
                SensorRecord r = SensorRecord.fromLine(line);
                // [CONFIA] assume que o datagrama é um registo válido vindo do líder:
                //   não verifica origem nem seq; se não tiver 4 campos ou números válidos
                //   lança IllegalArgumentException/NumberFormatException, que NÃO é
                //   apanhada pelo catch(IOException) e termina a réplica
                file.append(r);                                     // escreve sem verificar se falta algum registo anterior
                // [BLOQUEIA brevemente] escrita em disco
                System.out.println("Aplicado: " + r.toLine());
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}
