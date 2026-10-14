package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;

public class MulticastReplica {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        RecordFile file = new RecordFile("replica-" + id + ".txt");
        
        // Ponto 6: Estrutura para reter registos fora de ordem.
        // O TreeMap é ideal porque mantém as chaves (números de sequência) ordenadas automaticamente.
        Map<Long, SensorRecord> retidos = new TreeMap<>();
        
        // Variáveis para as medições da rajada (Ponto 7)
        long primeiroInstanteRececao = 0;
        long ultimoInstanteRececao = 0;
        int contRecebidos = 0;
        int contEscritos = 0;

        try (MulticastSocket socket = new MulticastSocket(PORT)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(group); 
            
            // Ponto 6: obter o último número de sequência aplicado
            long ultimo = file.lastSeq();
            System.out.println("Réplica " + id + " à escuta em " + GROUP + ":" + PORT + " (Último seq: " + ultimo + ")");
            
            byte[] buffer = new byte[1000];
            
            while (true) {
                DatagramPacket p = new DatagramPacket(buffer, buffer.length);
                socket.receive(p); 
                
                String line = new String(p.getData(), 0, p.getLength(), StandardCharsets.UTF_8);
                
                SensorRecord r;
                try {
                    // Ponto 6: A réplica tenta extrair os dados.
                    r = SensorRecord.fromLine(line);
                } catch (IllegalArgumentException e) {
                    // Ponto 6: Datagrama inválido. Rejeita e continua sem crashar.
                    System.out.println("REJEITADO: " + line);
                    continue; 
                }

                // Lógica de estatísticas para a Rajada (Ponto 7)
                if (contRecebidos == 0) {
                    primeiroInstanteRececao = System.currentTimeMillis();
                }
                ultimoInstanteRececao = System.currentTimeMillis();
                contRecebidos++;

                long seq = r.getSeq();

                // Ponto 6: A regra de ouro da aplicação!
                if (seq <= ultimo || retidos.containsKey(seq)) {
                    // 1. DUPLICADO: O registo é ignorado.
                    System.out.println("DUPLICADO: " + seq);
                } 
                else if (seq > ultimo + 1) {
                    // 2. FORA DE ORDEM: Chegou adiantado. Falta pelo menos um. Retém em memória.
                    retidos.put(seq, r);
                    System.out.println("EM ESPERA: " + seq + " (falta o registo " + (ultimo + 1) + ")");
                } 
                else if (seq == ultimo + 1) {
                    // 3. EM ORDEM: É exatamente o que esperávamos.
                    file.append(r);
                    ultimo++;
                    contEscritos++;
                    System.out.println("Aplicado: " + r.toLine());

                    // Entrega em cascata: verifica se, com o buraco tapado, podemos aplicar os retidos.
                    while (retidos.containsKey(ultimo + 1)) {
                        SensorRecord retido = retidos.remove(ultimo + 1);
                        file.append(retido);
                        ultimo++;
                        contEscritos++;
                        System.out.println("Aplicado (Retido): " + retido.toLine());
                    }
                }
                
                // (Opcional) Podes descomentar a linha abaixo para ver as estatísticas da rajada a correr na consola
                // System.out.printf("[Stats] Recebidos: %d | Escritos: %d | Retidos: %d | Latência Parcial: %d ms\n", 
                //        contRecebidos, contEscritos, retidos.size(), (ultimoInstanteRececao - primeiroInstanteRececao));
            }
        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}