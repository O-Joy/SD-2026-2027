package raft04;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class AlarmProcessor {
    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.out.println("Uso: AlarmProcessor <N> [api]");
            return;
        }

        int n = Integer.parseInt(args[0]);
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        IpfsPubSub ps = new IpfsPubSub(api);
        Partitioner partitioner = new Partitioner(n);

        // Ficheiro de log do monitor e array de monitores
        PrintWriter logFile = new PrintWriter(new FileWriter("monitor-processador.txt", true), true);
        HeartbeatMonitor[] monitores = new HeartbeatMonitor[n];
        
        for (int i = 0; i < n; i++) {
            monitores[i] = new HeartbeatMonitor(i, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, msg -> {
                String logMsg = System.currentTimeMillis() + ";" + msg;
                System.out.println(logMsg);
                logFile.println(logMsg);
            });
        }

        // Thread de verificação dos heartbeats
        ScheduledExecutorService verificador = Executors.newSingleThreadScheduledExecutor();
        verificador.scheduleAtFixedRate(() -> {
            for (HeartbeatMonitor m : monitores) m.check();
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        Object lock = new Object();
        long[] lp = {0};
        VectorClock vp = new VectorClock(n);
        
        // NOVIDADES COMINDIR05:
        Set<String> seen = ConcurrentHashMap.newKeySet(); // Guarda "particao#seq#termo"
        Map<Integer, Long> maxTermSeen = new ConcurrentHashMap<>(); // Maior termo visto por partição

        System.out.println("AlarmProcessor iniciado para " + n + " partições.");

        for (int k = 0; k < n; k++) {
            final int particaoOrigem = k;
            
            Thread t = new Thread(() -> {
                try {
                    ps.subscribe(Wire.topicData(particaoOrigem), msg -> {
                        try {
                            if (!Wire.type(msg).equals("LOTE")) return;
                            
                            Batch lote = Batch.decode(msg, n);
                            
                            // 1. Ignorar lotes com termos inferiores ao máximo já visto
                            long currentMaxTerm = maxTermSeen.getOrDefault(particaoOrigem, -1L);
                            if (lote.getTerm() < currentMaxTerm) return;
                            
                            maxTermSeen.put(particaoOrigem, lote.getTerm());

                            // 2. Avisa o monitor do sinal de vida com o Termo e o Líder
                            monitores[particaoOrigem].onHeartbeat(lote.getTerm(), lote.getLeader(), lote.getHb());

                            // 3. Processa os registos agrupados no lote
                            for (SensorRecord record : lote.getRecords()) {
                                long seq = record.getSeq();
                                long term = record.getTerm(); // <-- Agora usa o termo
                                String sensor = record.getSensorId();
                                double temp = record.getTemperature();
                                long lMsg = record.getLamport();
                                VectorClock vMsg = record.getVector();

                                if (sensor.startsWith("ALARM-")) continue;

                                // Identificador ÚNICO = particao + seq + termo
                                String msgId = particaoOrigem + "#" + seq + "#" + term;
                                long currentLp;
                                VectorClock currentVpCopy;

                                synchronized (lock) {
                                    if (!seen.add(msgId)) continue;
                                    lp[0] = Math.max(lp[0], lMsg);
                                    vp.merge(vMsg);
                                    currentLp = lp[0];
                                    currentVpCopy = vp.copy();
                                }

                                if (temp >= Wire.LIMIAR_ALARME) {
                                    String alarmSensor = "ALARM-" + sensor;
                                    int particaoDestino = partitioner.of(alarmSensor);

                                    // VERIFICAÇÃO COMINDIR05: Aviso sem líder conhecido
                                    if (monitores[particaoDestino].state() == HeartbeatMonitor.State.SUSPEITO) {
                                        System.out.println("AVISO: partição p" + particaoDestino + " sem líder conhecido; alarme " + alarmSensor + " pode perder-se");
                                    }

                                    String inMsg = Wire.in(alarmSensor, temp, currentLp, currentVpCopy);

                                    try {
                                        ps.publish(Wire.topicIn(particaoDestino), inMsg);
                                        System.out.println("ALARME: " + sensor + " p" + particaoOrigem + "#" + seq + "(t" + term + ") → p" + particaoDestino);
                                    } catch (IOException | InterruptedException e) {
                                        System.err.println("Falha ao publicar alarme: " + e.getMessage());
                                    }
                                }
                            }
                        } catch (Exception e) {
                            // Ignora falhas de parse de lotes mal formados
                        }
                    });
                } catch (IOException | InterruptedException e) {
                    System.err.println("Erro na subscrição da p" + particaoOrigem + ": " + e.getMessage());
                }
            });
            t.start();
        }
    }
}