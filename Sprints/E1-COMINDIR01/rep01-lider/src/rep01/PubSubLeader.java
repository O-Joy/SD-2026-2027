package rep01;

import java.io.*;
import java.util.Random;
import java.util.Scanner;

public class PubSubLeader {
    static final String TOPIC = "rep01-temperaturas";

    public static void main(String[] args) {
        try (Scanner sc = new Scanner(System.in)) {
            String api = args.length > 0 ? args[0] : "http://127.0.0.1:5001/api/v0";
            IpfsPubSub ipfs = new IpfsPubSub(api);
            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq();                                      // continua a numeração após reinício
            System.out.println("Líder pronto. Formato: <sensor> <temperatura> | 'rajada <n>' | 'inverter' | 'sair'");

            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;                               // bloqueia à espera do utilizador
                String line = sc.nextLine().trim();

                if (line.equalsIgnoreCase("sair")) break;

                // --- Modo de inversão (Pessoa B, ponto 6) ---
                if (line.equalsIgnoreCase("inverter")) {
                    System.out.println("A enviar os registos 2 e 3 antes do 1 para forçar retenção nas réplicas...");
                    long seqBase = seq;

                    SensorRecord r2 = SensorRecord.now(seqBase + 2, "TESTE", 20.0);
                    ipfs.publish(TOPIC, r2.toLine());

                    SensorRecord r3 = SensorRecord.now(seqBase + 3, "TESTE", 21.0);
                    ipfs.publish(TOPIC, r3.toLine());

                    try { Thread.sleep(1000); } catch (InterruptedException e) {}

                    SensorRecord r1 = SensorRecord.now(seqBase + 1, "TESTE", 19.0);
                    ipfs.publish(TOPIC, r1.toLine());

                    file.append(r1); file.append(r2); file.append(r3);
                    seq += 3;
                    continue;
                }

                // --- Modo rajada (Pessoa B, ponto 7) ---
                if (line.toLowerCase().startsWith("rajada")) {
                    String[] parts = line.split("\\s+");
                    if (parts.length == 2) {
                        try {
                            int n = Integer.parseInt(parts[1]);
                            Random rand = new Random();
                            System.out.println("A iniciar rajada de " + n + " registos...");

                            long startEnvio = System.currentTimeMillis();

                            for (int i = 0; i < n; i++) {
                                double temp = Math.round((15.0 + (15.0 * rand.nextDouble())) * 10.0) / 10.0;

                                SensorRecord r = SensorRecord.now(++seq, "SIM", temp);
                                file.append(r);                             // escreve localmente
                                ipfs.publish(TOPIC, r.toLine());            // propaga
                            }

                            long endEnvio = System.currentTimeMillis();
                            System.out.printf("Rajada concluída. Primeiro envio: %d, Último envio: %d (Duração: %d ms)\n",
                                    startEnvio, endEnvio, (endEnvio - startEnvio));
                        } catch (NumberFormatException e) {
                            System.out.println("Número de registos inválido.");
                        }
                    } else {
                        System.out.println("Formato: rajada <n>");
                    }
                    continue;
                }

                // --- Modo normal ---
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
                file.append(r);                                             // 1) escreve localmente
                ipfs.publish(TOPIC, r.toLine());                            // 2) propaga
                System.out.println("Registado e enviado: " + r.toLine());
            }
        } catch (IOException | InterruptedException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}