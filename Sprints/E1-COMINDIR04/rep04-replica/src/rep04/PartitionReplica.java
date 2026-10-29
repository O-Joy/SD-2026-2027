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

public class PartitionReplica {

    // ---- simulação de perdas: SÓ se aplica à receção por PubSub (nunca à recuperação por TCP) ----
    private static final double PERDA = Double.parseDouble(System.getProperty("perda", "0"));
    private static final int N = Integer.getInteger("n", 3);
    private static final long DESCARTAR = Long.parseLong(System.getProperty("descartar", "-1"));
    private static final AtomicBoolean DESCARTE_FEITO = new AtomicBoolean(false);   // descarta-se uma só vez
    private static final Random ALEATORIO = new Random();                           // Random é thread-safe

    // estado de UMA partição (cada partição tem o seu, e é usado por uma só thread)
    static class Estado {
        final int k;
        final RecordFile file;
        long ultimo;                                          // último seq escrito no ficheiro
        final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        String hostLider;                                     // atualizados a cada REG recebido
        int portoLider;

        Estado(int id, int k) throws IOException {
            this.k = k;
            this.file = new RecordFile("replica-" + id + "-p" + k + ".txt", N);
            this.ultimo = file.lastSeq();
        }

        // chamado pela thread de subscrição por cada mensagem do tópico de dados
        void onReg(String msg) {
            try {
                if (!Wire.type(msg).equals("REG")) {
                    System.out.println("p" + k + " REJEITADO: " + msg);
                    return;
                }
                String[] partes = Wire.regParts(msg);          // { endereço, porto, linha do registo }
                SensorRecord r = SensorRecord.fromLine(partes[2], N);
                int porto = Integer.parseInt(partes[1]);

                // simulação de perda (antes de aplicar e antes de aprender o endereço do líder)
                if (DESCARTAR == r.getSeq() && DESCARTE_FEITO.compareAndSet(false, true)) {
                    System.out.println("p" + k + " DESCARTADO (simulação): " + r.getSeq());
                    return;
                }
                if (PERDA > 0 && ALEATORIO.nextDouble() < PERDA) {
                    System.out.println("p" + k + " DESCARTADO (simulação): " + r.getSeq());
                    return;
                }

                hostLider = partes[0];
                portoLider = porto;
                aplicar(r, false);
                if (retidos.size() > Wire.LIMITE_RETIDOS) {
                    recuperar();
                }
            } catch (IllegalArgumentException e) {            // inclui NumberFormatException
                System.out.println("p" + k + " REJEITADO: " + msg);
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

        // escreve os retidos que passaram a estar em sequência
        void escreverEmSequencia() throws IOException {
            while (!retidos.isEmpty() && retidos.firstKey() == ultimo + 1) {
                SensorRecord r = retidos.pollFirstEntry().getValue();
                file.append(r);
                ultimo = r.getSeq();
                System.out.println("p" + k + " aplicado (estava retido): " + r.toLine());
            }
        }

        // pede ao líder o intervalo [ultimo+1, menor retido - 1] por TCP
        void recuperar() {
            long de = ultimo + 1;
            long ate = retidos.firstKey() - 1;
            System.out.println("p" + k + " RECUPERAÇÃO: " + de + ".." + ate
                    + " pedida a " + hostLider + ":" + portoLider);
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
                // readLine() devolveu null: o líder fechou a ligação sem enviar FIM
                System.out.println("p" + k + " RECUPERAÇÃO FALHOU: ligação fechada antes de FIM");
            } catch (IOException | IllegalArgumentException e) {
                // ligação recusada, tempo esgotado, interrupção ou linha inválida:
                // mantêm-se o ficheiro e os retidos; tenta-se de novo no próximo REG
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

        List<Thread> threads = new ArrayList<>();
        for (String s : args[1].split(",")) {
            final int k = Integer.parseInt(s.trim());
            final Estado estado = new Estado(id, k);              // um estado (e um ficheiro) por partição
            System.out.println("Réplica " + id + ": p" + k + " (último seq: " + estado.ultimo
                    + "), subscreve " + Wire.topicData(k));
            Thread t = new Thread(() -> {                         // uma thread por partição
                try {
                    ps.subscribe(Wire.topicData(k), estado::onReg);
                } catch (IOException | InterruptedException e) {
                    System.out.println("p" + k + " Subscrição terminada: " + e.getMessage());
                }
            }, "sub-p" + k);
            t.start();
            threads.add(t);
        }
        for (Thread t : threads) {                                // a thread principal aguarda
            t.join();
        }
    }
}