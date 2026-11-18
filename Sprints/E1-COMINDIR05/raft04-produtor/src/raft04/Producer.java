package raft04;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Producer {

    private static HeartbeatMonitor[] monitores;
    private static PrintWriter produtorLog; // NOVIDADE COMINDIR05: ficheiro de log das leituras

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Uso: Producer <N> [api]");
            return;
        }
        Partitioner part;
        try {
            part = new Partitioner(Integer.parseInt(args[0]));
        } catch (IllegalArgumentException e) {
            System.out.println("N inválido: " + args[0]);
            return;
        }
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";
        IpfsPubSub ps = new IpfsPubSub(api);
        Random aleatorio = new Random();
        Scanner teclado = new Scanner(System.in);
        
        int n = part.count();

        // 1. Configurar ficheiros de log e array de monitores
        PrintWriter logFile = new PrintWriter(new FileWriter("monitor-produtor.txt", true), true);
        produtorLog = new PrintWriter(new FileWriter("produtor.txt", true), true); // INICIALIZAR O LOG
        
        monitores = new HeartbeatMonitor[n];
        for (int i = 0; i < n; i++) {
            monitores[i] = new HeartbeatMonitor(i, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, msg -> {
                String logMsg = System.currentTimeMillis() + ";" + msg;
                System.out.println(logMsg);
                logFile.println(logMsg);
            });
        }

        // 2. Thread de verificação
        ScheduledExecutorService verificador = Executors.newSingleThreadScheduledExecutor();
        verificador.scheduleAtFixedRate(() -> {
            for (HeartbeatMonitor m : monitores) m.check();
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        // 3. Subscrever todos os LOTES apenas para manter o estado vivo
        for (int i = 0; i < n; i++) {
            final int k = i;
            new Thread(() -> {
                try {
                    ps.subscribe(Wire.topicData(k), msg -> {
                        try {
                            if (!Wire.type(msg).equals("LOTE")) return;
                            Batch lote = Batch.decode(msg, n);
                            // NOVIDADE COMINDIR05: onHeartbeat passa a receber o termo e o líder
                            monitores[k].onHeartbeat(lote.getTerm(), lote.getLeader(), lote.getHb());
                        } catch (Exception ignored) { }
                    });
                } catch (IOException | InterruptedException e) {
                    System.err.println("Erro a escutar p" + k + ": " + e.getMessage());
                }
            }).start();
        }

        System.out.println("Comandos: <sensor> <temperatura> | rajada <n> <s> | rajada-quente <n> <s> | sair");
        while (true) {
            System.out.print("> ");
            if (!teclado.hasNextLine()) {
                break;
            }
            String linha = teclado.nextLine();
            if (linha.isEmpty()) {
                continue;
            }
            if (linha.equals("sair")) {
                break;
            }
            String[] p = linha.split("\\s+");
            try {
                if (p[0].equals("rajada") || p[0].equals("rajada-quente")) {
                    rajada(ps, part, aleatorio, Integer.parseInt(p[1]), Integer.parseInt(p[2]),
                            p[0].equals("rajada-quente"));
                } else {
                    double temp = Double.parseDouble(p[1]);
                    int k = part.of(p[0]);
                    
                    // VERIFICAÇÃO COMINDIR05
                    if (monitores[k].state() == HeartbeatMonitor.State.SUSPEITO) {
                        System.out.println("AVISO: partição p" + k + " sem líder conhecido; leitura " + p[0] + " pode perder-se");
                    }
                    
                    long timeNow = System.currentTimeMillis();
                    produtorLog.println(timeNow + ";p" + k + ";" + p[0] + ";" + temp); // GRAVAR LEITURA
                    
                    ps.publish(Wire.topicIn(k), Wire.in(p[0], temp, 0 , new VectorClock(n)));
                    System.out.println(p[0] + " " + temp + " → p" + k);
                }
            } catch (NumberFormatException | ArrayIndexOutOfBoundsException e) {
                System.out.println("Linha inválida: " + linha);
            } catch (IOException e) {
                System.out.println("Falha ao publicar: " + e.getMessage());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        
        verificador.shutdownNow();
    }

    private static void rajada(IpfsPubSub ps, Partitioner part, Random aleatorio, int n, int numSensores,
                               boolean quente) throws IOException, InterruptedException {
        if (n < 1 || numSensores < 1) {
            System.out.println("n e s têm de ser positivos");
            return;
        }
        int[] porParticao = new int[part.count()];
        long primeiro = 0;
        long ultimo = 0;
        int falhas = 0;
        
        for (int i = 0; i < n; i++) {
            int num;
            if (quente) {
                num = (aleatorio.nextDouble() < 0.8 || numSensores < 2) ? 1 : 2 + aleatorio.nextInt(numSensores - 1);
            } else {
                num = 1 + aleatorio.nextInt(numSensores);
            }
            String sensor = String.format("S%02d", num);
            double temp = Math.round((15 + aleatorio.nextDouble() * 15) * 10) / 10.0;
            int k = part.of(sensor);
            
            // VERIFICAÇÃO COMINDIR05
            if (monitores[k].state() == HeartbeatMonitor.State.SUSPEITO) {
                System.out.println("AVISO: partição p" + k + " sem líder conhecido; leitura " + sensor + " pode perder-se");
            }
            
            if (i == 0) {
                primeiro = System.currentTimeMillis();
            }
            try {
                long timeNow = System.currentTimeMillis();
                produtorLog.println(timeNow + ";p" + k + ";" + sensor + ";" + temp); // GRAVAR LEITURA RAJADA
                
                ps.publish(Wire.topicIn(k), Wire.in(sensor, temp, 0 , new VectorClock(part.count())));
                porParticao[k]++;
            } catch (IOException e) {                       
                falhas++;
            }
            ultimo = System.currentTimeMillis();
        }
        System.out.println("Rajada de " + n + " leituras: primeiro envio " + primeiro
                + " ms, último envio " + ultimo + " ms (duração " + (ultimo - primeiro) + " ms)");
        StringBuilder sb = new StringBuilder("Leituras por partição:");
        for (int k = 0; k < porParticao.length; k++) {
            sb.append(" p").append(k).append("=").append(porParticao[k]);
        }
        sb.append(" | falhas ao publicar: ").append(falhas);
        System.out.println(sb);
    }
}