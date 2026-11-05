package rep04;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PartitionLeader {

    // estado partilhado pelas três atividades (entrada, lote/heartbeat e recuperação), protegido por LOCK
    private static final Object LOCK = new Object();
    private static long ultimoSeq;
    private static long L;
    private static VectorClock V;
    private static List<SensorRecord> pendentes = new ArrayList<>();   // registos com seq, ainda por publicar
    private static long hb;                                            // último heartbeat emitido nesta incarnação

    // incarnação: instante de arranque deste processo; só é escrita no arranque, antes de as threads começarem
    private static long inc;

    private static final boolean SILENCIO = Boolean.getBoolean("silencio");

    public static void main(String[] args) throws Exception {
        final int particao = Integer.parseInt(args[0]);
        final int numParticoes = Integer.parseInt(args[1]);
        final String endereco = args.length > 2 ? args[2] : "127.0.0.1";
        final String api = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";

        final IpfsPubSub ps = new IpfsPubSub(api);
        final RecordFile file = new RecordFile("lider-p" + particao + ".txt", numParticoes);
        final Path hbFile = Paths.get("hb-p" + particao + ".txt");
        final int porto = Wire.PORTO_BASE + particao;
        final Partitioner particionador = new Partitioner(numParticoes);

        ultimoSeq = file.lastSeq();
        final long inicial = ultimoSeq;

        // ---- relógios: recuperar L e V da última linha do ficheiro após reiniciar ----
        SensorRecord ultimo = file.last();
        if (ultimo == null) {
            L = 0;
            V = new VectorClock(numParticoes);
        } else {
            L = ultimo.getLamport();
            V = ultimo.getVector().copy();
        }

        // ---- heartbeat: nova incarnação em cada arranque, hb recomeça ----
        inc = System.currentTimeMillis();
        hb = 0;
        System.out.println("Líder da partição " + particao + ": incarnação inc=" + inc
                + ", período " + Wire.PERIODO_HB_MS + " ms (último seq: " + inicial + ", L=" + L + ", V=" + V + ")");

        // ---- servidor de recuperação aberto ANTES do primeiro heartbeat ----
        // o LOTE hb=1 pode anunciar um ultimoSeq com registos em falta (reinício da Figura 1); a réplica
        // liga-se logo a este porto, por isso ele já tem de estar a aceitar ligações (o SO guarda-as na
        // fila até ao accept()). Se o porto estiver ocupado, a BindException termina o líder aqui, antes
        // de haver outras threads a manter a JVM viva.
        final ServerSocket server = new ServerSocket(porto);

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
                        if (j != particao) {
                            System.out.println("PARTIÇÃO ERRADA: " + sensor + " pertence a p" + j);
                            return;
                        }

                        SensorRecord r;
                        long lEvento;
                        VectorClock vEvento;
                        synchronized (LOCK) {                       // relógios + seq + append + pendentes juntos
                            L = Math.max(L, lMsg) + 1;
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
                            pendentes.add(r);                       // fica à espera do próximo lote
                        }
                        System.out.println("EVENTO: seq=" + r.getSeq() + " L=" + lEvento + " V=" + vEvento);
                        if (!SILENCIO) {
                            System.out.println("Registado, pendente (p" + particao + "): " + r.toLine());
                        }
                    } catch (IllegalArgumentException e) {
                        System.out.println("REJEITADO: " + msg);
                    } catch (IOException e) {
                        System.out.println("Falha: " + e.getMessage());
                    }
                });
            } catch (IOException | InterruptedException e) {
                System.out.println("Subscrição terminada: " + e.getMessage());
            }
        }, "entrada");
        entrada.start();

        // ---- atividade 2: lote/heartbeat, no arranque e depois a cada PERIODO_HB_MS ----
        ScheduledExecutorService agendador = Executors.newSingleThreadScheduledExecutor(tarefa -> {
            Thread t = new Thread(tarefa, "heartbeat");
            return t;
        });
        agendador.scheduleAtFixedRate(() -> enviarLote(ps, particao, endereco, porto, hbFile),
                0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);

        // ---- atividade 3: recuperação (thread principal) ----
        try (server) {
            System.out.println("Líder da partição " + particao + " à escuta no porto " + porto
                    + " (último seq: " + inicial + ")");
            while (true) {
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

    // fecha o lote do intervalo e publica-o: é ao mesmo tempo o heartbeat (mesmo quando vazio)
    private static void enviarLote(IpfsPubSub ps, int particao, String endereco, int porto, Path hbFile) {
        // qualquer exceção que saísse daqui cancelaria as execuções seguintes do agendador:
        // por isso tudo é apanhado e assinalado, e o heartbeat seguinte sai na mesma
        try {
            List<SensorRecord> lote;
            long seqFecho;
            long hbAtual;
            synchronized (LOCK) {                       // troca de pendentes + leitura de ultimoSeq + hb juntos
                lote = pendentes;
                pendentes = new ArrayList<>();
                seqFecho = ultimoSeq;
                hbAtual = ++hb;
            }

            // fora do LOCK: a publicação é uma chamada de rede e não pode atrasar a entrada nem a recuperação
            Batch b = new Batch(endereco, porto, inc, hbAtual, seqFecho, lote);
            String msg = b.encode();
            int bytes = msg.getBytes(StandardCharsets.UTF_8).length;
            try {
                ps.publish(Wire.topicData(particao), msg);
            } catch (IOException e) {
                // não se volta a publicar: os participantes recuperam pelo ultimoSeq do lote seguinte
                System.out.println("Falha ao publicar LOTE hb=" + hbAtual + " (" + lote.size()
                        + " registos não publicados): " + e.getMessage());
                return;
            }

            // uma linha por lote publicado: inc;hb;instante;ultimoSeq;n;bytes
            long instante = System.currentTimeMillis();
            String linha = inc + ";" + hbAtual + ";" + instante + ";" + seqFecho + ";" + lote.size() + ";" + bytes;
            Files.writeString(hbFile, linha + System.lineSeparator(), StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);

            System.out.println("HEARTBEAT: hb=" + hbAtual + " n=" + lote.size() + " ultimoSeq=" + seqFecho);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            System.out.println("Falha no lote/heartbeat: " + e);
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