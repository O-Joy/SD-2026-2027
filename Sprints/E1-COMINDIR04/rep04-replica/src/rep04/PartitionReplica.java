package rep04;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PartitionReplica {

    // ---- simulação de perdas: SÓ se aplica à receção por PubSub (nunca à recuperação por TCP) ----
    private static final double PERDA = Double.parseDouble(System.getProperty("perda", "0"));
    private static final int N = Integer.getInteger("n", 3);
    private static final long DESCARTAR = Long.parseLong(System.getProperty("descartar", "-1"));
    private static final AtomicBoolean DESCARTE_FEITO = new AtomicBoolean(false);   // descarta-se uma só vez
    private static final Random ALEATORIO = new Random();                           // Random é thread-safe

    public static PrintWriter logFile;
    public static ScheduledExecutorService verificador;

    // estado de UMA partição (cada partição tem o seu, e é usado por uma só thread)
    static class Estado {
        final int k;
        final RecordFile file;
        long ultimo;                                          // último seq escrito no ficheiro
        final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        String hostLider;                                     // atualizados a cada LOTE recebido
        int portoLider;
        final HeartbeatMonitor monitor;

        Estado(int id, int k) throws IOException {
            this.k = k;
            this.file = new RecordFile("replica-" + id + "-p" + k + ".txt", N);
            this.ultimo = file.lastSeq();
            
            // Inicializa o detetor de falhas desta partição
            this.monitor = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT, msg -> {
                String logMsg = System.currentTimeMillis() + ";" + msg;
                System.out.println(logMsg);
                if (logFile != null) logFile.println(logMsg);
            });
        }

        // chamado pela thread de subscrição por cada mensagem do tópico de dados
        void onLote(String msg) {
            try {
                if (!Wire.type(msg).equals("LOTE")) {
                    System.out.println("p" + k + " REJEITADO (Não é LOTE): " + msg.split("\n")[0]);
                    return;
                }
                
                Batch lote = Batch.decode(msg, N);
                
                // 1. Avisa o monitor que chegou sinal de vida
                monitor.onHeartbeat(lote.getInc(), lote.getHb());

                // 2. Assinala a receção
                String logRecebido = System.currentTimeMillis() + ";RECEBIDO;p" + k + 
                                     ";inc=" + lote.getInc() + ";hb=" + lote.getHb() + 
                                     ";ultimoSeq=" + lote.getLastSeq() + ";n=" + lote.getRecords().size() + 
                                     ";intervalo=" + lote.seqRange();
                System.out.println(logRecebido);
                if (logFile != null) logFile.println(logRecebido);

                hostLider = lote.getAddress();
                portoLider = lote.getPort();

                // 3. Aplica os registos do lote
                for (SensorRecord r : lote.getRecords()) {
                    // simulação de perda (antes de aplicar)
                    if (DESCARTAR == r.getSeq() && DESCARTE_FEITO.compareAndSet(false, true)) {
                        System.out.println("p" + k + " DESCARTADO (simulação): " + r.getSeq());
                        continue;
                    }
                    if (PERDA > 0 && ALEATORIO.nextDouble() < PERDA) {
                        System.out.println("p" + k + " DESCARTADO (simulação): " + r.getSeq());
                        continue;
                    }
                    aplicar(r, false);
                }

                // 4. Recuperação: verifica se, no fim de aplicar, falta algo até ao ultimoSeq do líder
                if (ultimo < lote.getLastSeq()) {
                    recuperar(lote.getLastSeq());
                }

            } catch (IllegalArgumentException e) {
                System.out.println("p" + k + " REJEITADO: LOTE mal formado");
            } catch (IOException e) {
                System.out.println("p" + k + " Falha ao escrever: " + e.getMessage());
            }
        }

        // regra de aplicação (igual para registos do PubSub e do TCP)
        void aplicar(SensorRecord r, boolean porTcp) throws IOException {
            long seq = r.getSeq();
            if (seq <= ultimo || retidos.containsKey(seq)) {
                System.out.println("p" + k + " DUPLICADO: " + seq);
            } else if (seq == ultimo + 1) {
                file.append(r);
                ultimo = seq;
                System.out.println(porTcp ? "p" + k + " RECUPERADO: " + seq
                        : "p" + k + " aplicado: " + r.toLine());
                escreverEmSequencia();
            } else {
                retidos.put(seq, r);
                System.out.println("p" + k + " EM ESPERA: " + seq + " (falta o registo " + (ultimo + 1) + ")");
            }
        }

        void escreverEmSequencia() throws IOException {
            while (!retidos.isEmpty() && retidos.firstKey() == ultimo + 1) {
                SensorRecord r = retidos.pollFirstEntry().getValue();
                file.append(r);
                ultimo = r.getSeq();
                System.out.println("p" + k + " aplicado (estava retido): " + r.toLine());
            }
        }

        // pede ao líder o intervalo [ultimo+1, alvo] por TCP
        void recuperar(long ultimoSeqLider) {
            long de = ultimo + 1;
            // Pede até ao primeiro retido - 1, ou até ao ultimoSeq se não tiver retidos
            long ate = retidos.isEmpty() ? ultimoSeqLider : Math.min(ultimoSeqLider, retidos.firstKey() - 1);
            
            if (de > ate) return; // Nada a recuperar neste intervalo

            String logRec = System.currentTimeMillis() + ";RECUPERACAO;p" + k + ";de=" + de + ";ate=" + ate;
            System.out.println(logRec);
            if (logFile != null) logFile.println(logRec);

            System.out.println("p" + k + " RECUPERAÇÃO TCP (" + de + ".." + ate + ") pedida a " + hostLider + ":" + portoLider);
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(hostLider, portoLider), Wire.T_TCP_MS);
                s.setSoTimeout(Wire.T_TCP_MS);
                PrintWriter out = new PrintWriter(
                        new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);
                BufferedReader in = new BufferedReader(
                        new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                out.println("PEDIDO " + de + " " + ate);

                String linha;
                while ((linha = in.readLine()) != null) {
                    if (linha.equals("FIM")) {
                        escreverEmSequencia();
                        return;
                    }
                    if (linha.startsWith("ERRO")) {
                        System.out.println("p" + k + " RECUPERAÇÃO FALHOU: " + linha);
                        return;
                    }
                    aplicar(SensorRecord.fromLine(linha, N), true);
                }
                System.out.println("p" + k + " RECUPERAÇÃO FALHOU: ligação fechada antes de FIM");
            } catch (IOException | IllegalArgumentException e) {
                System.out.println("p" + k + " RECUPERAÇÃO FALHOU: " + e);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: PartitionReplica <id> <k1,k2,...> [portoApi]");
            return;
        }
        int id = Integer.parseInt(args[0]);
        String api = "http://127.0.0.1:" + (args.length > 2 ? args[2] : "5001") + "/api/v0";
        IpfsPubSub ps = new IpfsPubSub(api);

        logFile = new PrintWriter(new FileWriter("monitor-replica" + id + ".txt", true), true);
        
        List<Thread> threads = new ArrayList<>();
        List<Estado> estadosAtivos = new ArrayList<>();

        for (String s : args[1].split(",")) {
            final int k = Integer.parseInt(s.trim());
            final Estado estado = new Estado(id, k);
            estadosAtivos.add(estado);
            
            System.out.println("Réplica " + id + ": p" + k + " (último seq: " + estado.ultimo + "), subscreve " + Wire.topicData(k));
            Thread t = new Thread(() -> { 
                try {
                    ps.subscribe(Wire.topicData(k), estado::onLote);
                } catch (IOException | InterruptedException e) {
                    System.out.println("p" + k + " Subscrição terminada: " + e.getMessage());
                }
            }, "sub-p" + k);
            t.start();
            threads.add(t);
        }

        // Inicia a thread de verificação dos monitores a cada VERIFICACAO_MS
        verificador = Executors.newSingleThreadScheduledExecutor();
        verificador.scheduleAtFixedRate(() -> {
            for (Estado e : estadosAtivos) {
                e.monitor.check();
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        for (Thread t : threads) {
            t.join();
        }
    }
}