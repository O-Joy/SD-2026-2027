package rep03;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PartitionLeader {

    // estado partilhado pelas duas atividades
    private static final Object LOCK = new Object();
    private static long ultimoSeq;
    private static long L;
    private static VectorClock V;
    private static final boolean SILENCIO = Boolean.getBoolean("silencio");

    public static void main(String[] args) throws Exception {
        final int particao = Integer.parseInt(args[0]);
        final int numParticoes = Integer.parseInt(args[1]);          // só é usado no ponto 9
        final String endereco = args.length > 2 ? args[2] : "127.0.0.1";
        final String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";

        final IpfsPubSub ps = new IpfsPubSub(api);
        final RecordFile file = new RecordFile("lider-p" + particao + ".txt", numParticoes);
        final int porto = Wire.PORTO_BASE + particao;
        final Partitioner particionador = new Partitioner(numParticoes);

        ultimoSeq = file.lastSeq();
        final long inicial = ultimoSeq;

        // ---- rELÓGIO: recuperar relógios após reiniciar ----
        SensorRecord ultimo = file.last();
        if(ultimo == null) {
            L = 0;
            V= new VectorClock(numParticoes);
        }
        else {
            L = ultimo.getLamport();
            V = ultimo.getVector().copy();
        }

        // ---- atividade 1: entrada (thread própria, porque o subscribe nunca termina) ----
        Thread entrada = new Thread(() -> {
            try {
                ps.subscribe(Wire.topicIn(particao), msg -> {
                    try {
                        if (!Wire.type(msg).equals("IN")) {
                            System.out.println("REJEITADO: " + msg);
                            return;
                        }
                        String[] partes = Wire.body(msg).split(";");
                        if (partes.length != 4) {
                            System.out.println("REJEITADO: " + msg);
                            return;
                        }

                        String sensor = partes[0];
                        double temp = Double.parseDouble(partes[1]);
                        long lMsg = Long.parseLong(partes[2]);
                        VectorClock vMsg = VectorClock.parse(partes[3], numParticoes);

                        int j = particionador.of(sensor);
                        if(j != particao){
                            System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + j);
                            return;
                        }

                        SensorRecord r;
                        long lEvento;
                        VectorClock vEvento;
                        synchronized (LOCK) {                       // seq + append juntos
                            L = Math.max(L, lMsg) + 1 ;
                            V.merge(vMsg);
                            V.tick(particao);
                            ultimoSeq++;
                            if (V.get(particao) != ultimoSeq) {
                                System.out.println("Erro: V[" + particao + "] " + V.get(particao) + " != seq=" + ultimoSeq);
                            }
                            vEvento = V.copy();
                            lEvento = L;
                            r = SensorRecord.now(ultimoSeq, sensor, temp, lEvento, vEvento);
                            file.append(r);
                        }
                        System.out.println("EVENTO: seq=" + r.getSeq() + " L=" + lEvento + " V=" + vEvento);
                        ps.publish(Wire.topicData(particao), Wire.reg(endereco, porto, r));
                        if (!SILENCIO) {
                            System.out.println("Registado e enviado (p" + particao + "): " + r.toLine());
                        }
                    } catch (IllegalArgumentException e) {
                        System.out.println("REJEITADO: " + msg);
                    } catch (IOException | InterruptedException e) {
                        System.out.println("Falha: " + e.getMessage());
                    }
                });
            } catch (IOException | InterruptedException e) {
                System.out.println("Subscrição terminada: " + e.getMessage());
            }
        });
        entrada.start();

        // ---- atividade 2: recuperação (thread principal) ----
        try (ServerSocket server = new ServerSocket(porto)) {
            System.out.println("Líder da partição " + particao + " à escuta no porto " + porto
                    + " (último seq: " + inicial + ")");
            while(true) {
                try (Socket s = server.accept();
                     BufferedReader in = new BufferedReader(
                             new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                     PrintWriter out = new PrintWriter(
                             new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {
                    s.setSoTimeout(Wire.T_TCP_MS);
                    String pedido = in.readLine();
                    atender(pedido, file, out, s.getInetAddress().getHostAddress());
                } catch (IOException e) {
                    System.out.println("Recuperação interrompida: " + e.getMessage());
                }
            }
        }
    }

    // valida "PEDIDO <de> <até>" e responde com os registos + FIM, ou com ERRO
    private static void atender(String pedido, RecordFile file, PrintWriter out, String remoto)
            throws IOException {
        String[] p = pedido == null ? new String[0] : pedido.trim().split("\\s+");
        if (p.length != 3 || !p[0].equals("PEDIDO")) {
            out.println("ERRO pedido mal formado");
            return;
        }
        long de, ate;
        try {
            de = Long.parseLong(p[1]);
            ate = Long.parseLong(p[2]);
        } catch (NumberFormatException e) {
            out.println("ERRO valores inválidos");
            return;
        }
        if (de > ate) {
            out.println("ERRO intervalo inválido");
            return;
        }
        long ultimo;
        synchronized (LOCK) {
            ultimo = ultimoSeq;
        }

        ate = Math.min(ate, ultimo);                       // nunca promete o que ainda não existe

        List<SensorRecord> registos = file.range(de, ate);
        for (SensorRecord r : registos) {
            out.println(r.toLine());
        }
        out.println("FIM");
        System.out.println("RECUPERAÇÃO: " + de + ".." + ate + " para " + remoto
                + " (" + registos.size() + " registos)");
    }
}