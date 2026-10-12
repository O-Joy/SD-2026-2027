package rep01;

import java.io.*;
import java.util.Map;
import java.util.TreeMap;

public class PubSubReplica {
    static final String TOPIC = "rep01-temperaturas";

    // estado partilhado com a função chamada em cada mensagem (por isso, campos static)
    static RecordFile file;
    static final Map<Long, SensorRecord> retidos = new TreeMap<>();
    static long ultimo;
    static long primeiroInstanteRececao = 0;
    static long ultimoInstanteRececao = 0;
    static int contRecebidos = 0;
    static int contEscritos = 0;

    public static void main(String[] args) {
        String id = args.length > 0 ? args[0] : "1";
        String porto = args.length > 1 ? args[1] : "5001";
        String api = "http://127.0.0.1:" + porto + "/api/v0";
        file = new RecordFile("replica-" + id + ".txt");

        try {
            ultimo = file.lastSeq();
            System.out.println("Réplica " + id + " à escuta no tópico " + TOPIC
                    + " (API " + api + ", último seq: " + ultimo + ")");
            IpfsPubSub ipfs = new IpfsPubSub(api);
            ipfs.subscribe(TOPIC, line -> {
                try {
                    processar(line);
                } catch (IOException e) {
                    System.out.println("IO: " + e.getMessage());
                }
            });
        } catch (IOException | InterruptedException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }

    static void processar(String line) throws IOException {
        SensorRecord r;
        try {
            r = SensorRecord.fromLine(line);
        } catch (IllegalArgumentException e) {
            System.out.println("REJEITADO: " + line);
            return;
        }

        // estatísticas da rajada (ponto 7)
        if (contRecebidos == 0) {
            primeiroInstanteRececao = System.currentTimeMillis();
        }
        ultimoInstanteRececao = System.currentTimeMillis();
        contRecebidos++;

        long seq = r.getSeq();

        if (seq <= ultimo || retidos.containsKey(seq)) {
            System.out.println("DUPLICADO: " + seq);
        } else if (seq > ultimo + 1) {
            retidos.put(seq, r);
            System.out.println("EM ESPERA: " + seq + " (falta o registo " + (ultimo + 1) + ")");
        } else {
            file.append(r);
            ultimo++;
            contEscritos++;
            System.out.println("Aplicado: " + r.toLine());

            while (retidos.containsKey(ultimo + 1)) {
                SensorRecord retido = retidos.remove(ultimo + 1);
                file.append(retido);
                ultimo++;
                contEscritos++;
                System.out.println("Aplicado (Retido): " + retido.toLine());
            }
        }

        if (contRecebidos % 100 == 0) {
            System.out.printf("[Stats] Recebidos: %d | Escritos: %d | Retidos: %d | 1.º: %d | último: %d | Latência parcial: %d ms%n",
                    contRecebidos, contEscritos, retidos.size(),
                    primeiroInstanteRececao, ultimoInstanteRececao,
                    (ultimoInstanteRececao - primeiroInstanteRececao));
        }
    }
}