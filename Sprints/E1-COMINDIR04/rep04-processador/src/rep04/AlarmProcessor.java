package rep04;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class AlarmProcessor {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Uso: AlarmProcessor <N> [api]");
            return;
        }

        int n = Integer.parseInt(args[0]);
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        IpfsPubSub ps = new IpfsPubSub(api);
        Partitioner partitioner = new Partitioner(n);

        // Estado partilhado entre as threads de subscrição
        Object lock = new Object();
        long[] lp = {0}; // Array de 1 posição para ser mutável dentro do lambda
        VectorClock vp = new VectorClock(n);
        Set<String> seen = new HashSet<>(); // Para detetar registos duplicados

        System.out.println("AlarmProcessor iniciado para " + n + " partições.");

        for (int k = 0; k < n; k++) {
            final int particaoOrigem = k;
            
            Thread t = new Thread(() -> {
                try {
                    ps.subscribe(Wire.topicData(particaoOrigem), msg -> {
                        try {
                            // Formato esperado do Líder (Pessoa A): 
                            // REG;<endereço>;<porto>;<seq>;<sensor>;<temperatura>;<instante>;<L>;<V>
                            String[] parts = msg.split(";");
                            if (!parts[0].equals("REG") || parts.length < 9) return;

                            long seq = Long.parseLong(parts[3]);
                            String sensor = parts[4];
                            double temp = Double.parseDouble(parts[5]);
                            long lMsg = Long.parseLong(parts[7]);
                            VectorClock vMsg = VectorClock.parse(parts[8], n);

                            // Ignorar alarmes gerados pelo próprio processador
                            if (sensor.startsWith("ALARM-")) {
                                return;
                            }

                            String msgId = particaoOrigem + "#" + seq;
                            long currentLp;
                            VectorClock currentVpCopy;

                            // Secção crítica: protege a atualização dos relógios e o registo de duplicados
                            synchronized (lock) {
                                if (!seen.add(msgId)) {
                                    return; // Registo duplicado, ignorar
                                }

                                // Atualiza o conhecimento causal do processador
                                lp[0] = Math.max(lp[0], lMsg);
                                vp.merge(vMsg);

                                // Faz um snapshot do estado causal EXATO deste momento
                                currentLp = lp[0];
                                currentVpCopy = vp.copy();
                            }

                            // Verifica se atinge o limiar para gerar novo evento
                            if (temp >= Wire.LIMIAR_ALARME) {
                                String alarmSensor = "ALARM-" + sensor;
                                int particaoDestino = partitioner.of(alarmSensor);

                                // Formato IN esperado pelo Líder (Pessoa A): IN;<sensor>;<temperatura>;<L>;<V>
                                String inMsg = "IN;" + alarmSensor + ";" + temp + ";" + currentLp + ";" + currentVpCopy.toString();

                                try {
                                    ps.publish(Wire.topicIn(particaoDestino), inMsg);
                                    System.out.println("ALARME: " + sensor + " p" + particaoOrigem + "#" + seq + " → p" + particaoDestino);
                                } catch (IOException | InterruptedException e) {
                                    System.err.println("Falha ao publicar alarme: " + e.getMessage());
                                }
                            }
                        } catch (Exception e) {
                            // Falha no parsing da mensagem, continua à escuta
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