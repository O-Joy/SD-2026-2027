package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Random; // <-- Adicionado para a rajada
import java.util.Scanner;

// Ponto 2 — código do enunciado. Os comentários [BLOQUEIA] respondem ao "Antes de avançar" do ponto 3.
public class MulticastLeader {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        try (MulticastSocket socket = new MulticastSocket();                // porto efémero: o líder só envia, não se junta ao grupo
             Scanner sc = new Scanner(System.in)) {
            InetAddress group = InetAddress.getByName(GROUP);
            socket.setTimeToLive(1);                                        // o datagrama não sai da rede local
            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq();                                      // continua a numeração após reinício
            // [BLOQUEIA brevemente] leitura do disco: espera pelo SO a ler lider.txt
            System.out.println("Líder pronto. Formato: <sensor> <temperatura> | 'rajada <n>' | 'inverter' | 'sair'");
            
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;                       // bloqueia à espera do utilizador
                // [BLOQUEIA] espera indefinidamente que o utilizador escreva uma linha
                //            (ou que o stdin feche — EOF, Ctrl+D/Ctrl+Z)
                String line = sc.nextLine().trim();
                
                if (line.equalsIgnoreCase("sair")) break;

                // ====================================================================
                // --- A TUA PARTE (PESSOA B): PONTO 6 - MODO DE INVERSÃO ---
                if (line.equalsIgnoreCase("inverter")) {
                    System.out.println("A enviar os registos 2 e 3 antes do 1 para forçar retenção nas réplicas...");
                    long seqBase = seq;
                    
                    // Cria e envia o 2 e o 3 primeiro (desordem provocada)
                    SensorRecord r2 = SensorRecord.now(seqBase + 2, "TESTE", 20.0);
                    byte[] m2 = r2.toLine().getBytes(StandardCharsets.UTF_8);
                    socket.send(new DatagramPacket(m2, m2.length, group, PORT));
                    
                    SensorRecord r3 = SensorRecord.now(seqBase + 3, "TESTE", 21.0);
                    byte[] m3 = r3.toLine().getBytes(StandardCharsets.UTF_8);
                    socket.send(new DatagramPacket(m3, m3.length, group, PORT));
                    
                    try { Thread.sleep(1000); } catch (InterruptedException e) {}
                    
                    // Por fim, envia o 1
                    SensorRecord r1 = SensorRecord.now(seqBase + 1, "TESTE", 19.0);
                    byte[] m1 = r1.toLine().getBytes(StandardCharsets.UTF_8);
                    socket.send(new DatagramPacket(m1, m1.length, group, PORT));
                    
                    // No ficheiro do líder, gravamos na ordem correta
                    file.append(r1); file.append(r2); file.append(r3);
                    seq += 3;
                    continue;
                }

                // --- A TUA PARTE (PESSOA B): PONTO 7 - MODO RAJADA ---
                if (line.toLowerCase().startsWith("rajada")) {
                    String[] parts = line.split("\\s+");
                    if (parts.length == 2) {
                        try {
                            int n = Integer.parseInt(parts[1]);
                            Random rand = new Random();
                            System.out.println("A iniciar rajada de " + n + " registos...");
                            
                            long startEnvio = System.currentTimeMillis();
                            
                            for (int i = 0; i < n; i++) {
                                // Gera temp entre 15.0 e 30.0 arredondada a 1 casa decimal
                                double temp = Math.round((15.0 + (15.0 * rand.nextDouble())) * 10.0) / 10.0;
                                
                                SensorRecord r = SensorRecord.now(++seq, "SIM", temp);
                                file.append(r); // Escreve localmente
                                
                                byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);
                                socket.send(new DatagramPacket(m, m.length, group, PORT));
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
                // ====================================================================

                // Modo normal (código da Pessoa A)
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