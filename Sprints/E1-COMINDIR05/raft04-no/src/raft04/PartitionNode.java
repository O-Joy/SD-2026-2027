package raft04;

import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Nó de partição (COMINDIR05): junta o PartitionLeader e a PartitionReplica da COMINDIR04.
 *
 *   PartitionNode <k> <i> [N]
 *
 * LOCKS
 *   - monitor do nó (synchronized (this)): estado do REGISTO — ultimoSeq, ultimoTermo, seq, L, V,
 *     hb, pendentes, loteTask — e todas as chamadas ao RaftState que podem mudar o termo/papel;
 *   - monitor do RaftState: estado da ELEIÇÃO — term, votedFor, role, leader, votes, deadline.
 *   Ordem de aquisição, SEMPRE: nó -> raft. O RaftState nunca chama nada sincronizado do nó
 *   (só chama log(), que NÃO é synchronized), por isso não há ordem inversa nem deadlock.
 */
public class PartitionNode {

    // ---- identidade ----
    private final int k;          // partição de dados
    private final int i;          // número do nó no grupo (1..GRUPO)
    private final int n;          // número de partições N (dimensão dos vetores)

    // ---- infraestrutura ----
    private final IpfsPubSub ps = new IpfsPubSub("http://127.0.0.1:5001/api/v0");
    private final Partitioner particionador;
    private final RecordFile file;                 // no-p<k>-n<i>.txt
    private final PrintWriter eventos;             // eventos-p<k>-n<i>.txt
    private final PrintWriter hbLog;               // hb-p<k>-n<i>.txt
    private final RaftState raft;                  // estado-p<k>-n<i>.txt
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    // ---- estado do registo (protegido por synchronized (this)) ----
    private long ultimoSeq = 0;                    // seq do último registo do ficheiro
    private long ultimoTermo = 0;                  // termo desse registo

    // ---- estado de líder (protegido por synchronized (this)) ----
    private long seq;                              // último seq atribuído (= ultimoSeq enquanto líder)
    private long lamport;                          // L
    private VectorClock vector;                    // V
    private long hb;                               // número do heartbeat neste termo
    private List<SensorRecord> pendentes = new ArrayList<>();
    private ScheduledFuture<?> loteTask;           // agendamento do enviarLote()

    public PartitionNode(int k, int i, int n) throws IOException {
        this.k = k;
        this.i = i;
        this.n = n;
        this.particionador = new Partitioner(n);
        this.file = new RecordFile("no-p" + k + "-n" + i + ".txt", n);
        this.eventos = new PrintWriter(new FileWriter("eventos-p" + k + "-n" + i + ".txt", true), true);
        this.hbLog = new PrintWriter(new FileWriter("hb-p" + k + "-n" + i + ".txt", true), true);
        carregarUltimoRegisto();
        // arranca SEMPRE como seguidor, com o termo e o voto gravados em estado-p<k>-n<i>.txt
        this.raft = new RaftState(k, i, Wire.GRUPO, Wire.ELEICAO_MIN_MS, Wire.ELEICAO_MAX_MS,
                Path.of("estado-p" + k + "-n" + i + ".txt"), this::log);
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: PartitionNode <k> <i> [N]");
            return;
        }
        int k = Integer.parseInt(args[0]);
        int i = Integer.parseInt(args[1]);
        int n = args.length > 2 ? Integer.parseInt(args[2]) : 3;
        new PartitionNode(k, i, n).start();
    }

    // evento: "instante;EVENTO;p<k>;detalhe" na consola e em eventos-p<k>-n<i>.txt
    // NÃO é synchronized de propósito: é chamado de dentro do RaftState (ordem raft -> nó proibida)
    void log(String msg) {
        String linha = System.currentTimeMillis() + ";" + msg;
        System.out.println(linha);
        eventos.println(linha);                   // PrintWriter já é thread-safe
    }

    // lê o último registo do ficheiro e atualiza ultimoSeq e ultimoTermo
    private synchronized void carregarUltimoRegisto() throws IOException {
        SensorRecord ultimo = file.last();
        ultimoSeq = (ultimo == null) ? 0 : ultimo.getSeq();
        ultimoTermo = (ultimo == null) ? 0 : ultimo.getTerm();
    }

    private void start() throws Exception {
        log("ARRANQUE;p" + k + ";n" + i + ";termo=" + raft.term()
                + ";ultimoSeq=" + ultimoSeq + ";ultimoTermo=" + ultimoTermo);

        iniciarServidorRecuperacao();

        // thread de verificação: tick() a cada VERIFICACAO_MS
        scheduler.scheduleAtFixedRate(this::verificar,
                Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        // o subscribe NUNCA termina: cada tópico precisa da sua thread
        subscrever(Wire.topicElection(k), this::onEleicao);
        subscrever(Wire.topicData(k), this::onLote);
        subscrever(Wire.topicIn(k), this::onIn);     // o tópico IN não é afetado pelo isolamento
    }

    private void subscrever(String topic, Consumer<String> handler) {
        new Thread(() -> {
            try {
                ps.subscribe(topic, msg -> {
                    try {
                        handler.accept(msg);
                    } catch (RuntimeException e) {     // uma mensagem má não mata a subscrição
                        log("ERRO;p" + k + ";" + topic + ": " + e);
                    }
                });
            } catch (IOException | InterruptedException e) {
                log("ERRO;p" + k + ";subscrição de " + topic + " terminou: " + e.getMessage());
            }
        }, "sub-" + topic).start();
    }

    // ================================================================ ELEIÇÃO

    private void verificar() {
        try {
            if (Wire.isolated(k, i)) return;           // isolado: não é candidato nem publica
            long t, s, tr;
            synchronized (this) {                      // nó -> raft
                t = raft.tick();                       // grava termo e voto ANTES de publicar
                s = ultimoSeq;                         // valores ATUAIS do registo
                tr = ultimoTermo;
            }
            if (t != -1) {
                publicar(Wire.topicElection(k), new VoteRequest(t, i, s, tr).encode());
            }
        } catch (RuntimeException e) {                 // uma exceção cancelaria o agendamento
            log("ERRO;p" + k + ";verificação: " + e);
        }
    }

    private void onEleicao(String msg) {
        if (Wire.isolated(k, i)) return;
        if (msg.startsWith("PEDIDO_VOTO")) {
            VoteRequest vr = VoteRequest.decode(msg);
            if (vr.candidate == i) return;                         // a própria mensagem
            RaftState.Vote voto;
            synchronized (this) {                                  // nó -> raft
                // pode adotar um termo superior e demitir o nó: fica serializado com os IN
                voto = raft.onVoteRequest(vr.term, vr.candidate, vr.lastSeq, vr.lastTerm,
                        ultimoSeq, ultimoTermo);
            }
            // responde SEMPRE, mesmo a recusar: leva o termo atual de volta ao candidato
            publicar(Wire.topicElection(k), new VoteReply(voto.term, i, vr.candidate, voto.granted).encode());
        } else if (msg.startsWith("VOTO")) {
            VoteReply rep = VoteReply.decode(msg);
            if (rep.voter == i || rep.candidate != i) return;      // a própria, ou não é para mim
            synchronized (this) {                                  // nó -> raft
                if (raft.onVote(rep.term, rep.voter, rep.candidate, rep.granted)) {
                    tornarLider();
                }
            }
        }
    }

    // ================================================================ LÍDER (ponto 4)

    // chamado com o monitor do nó já adquirido (dentro de onEleicao)
    private synchronized void tornarLider() {
        try {
            SensorRecord ultimo = file.last();
            seq = ultimoSeq;                                       // continua a numeração do grupo
            if (ultimo == null) {                                  // registo vazio (1.º termo)
                lamport = 0;
                vector = new VectorClock(n);
            } else {                                               // continua os relógios: V[k] = seq
                lamport = ultimo.getLamport();
                vector = ultimo.getVector().copy();                // cópia: o registo não pode mudar
            }
            hb = 0;
            pendentes = new ArrayList<>();
            if (loteTask != null) loteTask.cancel(false);
            // 1.º LOTE de imediato: afirma a liderança perante os outros nós
            loteTask = scheduler.scheduleAtFixedRate(this::enviarLote,
                    0, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
            log("INICIO_LIDER;p" + k + ";termo=" + raft.term() + ";seq=" + seq
                    + ";L=" + lamport + ";V=" + vector);
        } catch (IOException e) {
            log("ERRO;p" + k + ";passagem a líder: " + e.getMessage());
        }
    }

    // IN;<sensor>;<temperatura>;<L>;<V>
    private void onIn(String msg) {
        String sensor;
        double temp;
        long lMsg;
        VectorClock vMsg;
        try {
            if (!Wire.type(msg).equals("IN")) return;
            String[] p = Wire.body(msg).split(";");
            if (p.length != 4) throw new IllegalArgumentException("campos");
            sensor = p[0];
            temp = Double.parseDouble(p[1]);
            lMsg = Long.parseLong(p[2]);
            vMsg = VectorClock.parse(p[3], n);
        } catch (IllegalArgumentException e) {
            log("ERRO;p" + k + ";IN mal formado: " + msg);
            return;
        }
        if (particionador.of(sensor) != k) {
            log("ERRO;p" + k + ";IN de " + sensor + " pertence a p" + particionador.of(sensor));
            return;
        }

        synchronized (this) {                       // termo + relógios + seq + append + pendentes juntos
            long t = raft.termIfLeader();           // nó -> raft
            if (t == -1) {
                log("IGNORADO;p" + k + ";IN " + sensor + " " + temp + " (nao sou lider)");
                return;
            }
            lamport = Math.max(lamport, lMsg) + 1;
            vector.merge(vMsg);
            vector.tick(k);
            seq++;
            if (vector.get(k) != seq) {
                log("ERRO;p" + k + ";V[" + k + "]=" + vector.get(k) + " != seq=" + seq);
            }
            SensorRecord r = SensorRecord.now(seq, t, sensor, temp, lamport, vector.copy());
            try {
                file.append(r);
            } catch (IOException e) {
                seq--;                              // não ficou no ficheiro: não conta
                log("ERRO;p" + k + ";escrita: " + e.getMessage());
                return;
            }
            ultimoSeq = seq;
            ultimoTermo = t;
            pendentes.add(r);                       // sai no próximo LOTE
            System.out.println("EVENTO: seq=" + seq + " termo=" + t + " L=" + lamport + " V=" + vector);
        }
    }

    private void enviarLote() {
        // qualquer exceção que saísse daqui cancelaria as execuções seguintes do agendador
        try {
            long t, s, tr, h;
            List<SensorRecord> lote;
            synchronized (this) {                   // termo + troca de pendentes + ultimoSeq + hb juntos
                t = raft.termIfLeader();            // nó -> raft
                if (t == -1) {                      // demitiu-se: deixa de enviar lotes
                    if (loteTask != null) loteTask.cancel(false);
                    loteTask = null;
                    pendentes = new ArrayList<>();  // estes registos nunca serão publicados
                    return;
                }
                lote = pendentes;
                pendentes = new ArrayList<>();
                s = ultimoSeq;
                tr = ultimoTermo;
                h = ++hb;
            }
            // FORA da secção: publicar é uma chamada de rede e não pode bloquear os IN
            Batch b = new Batch(t, i, "127.0.0.1", Wire.nodePort(k, i), h, s, tr, lote);
            String msg = b.encode();
            if (!publicar(Wire.topicData(k), msg)) return;      // isolado ou falha: não foi publicado
            int bytes = msg.getBytes(StandardCharsets.UTF_8).length;
            // termo;hb;instante;ultimoSeq;ultimoTermo;n;bytes
            hbLog.println(t + ";" + h + ";" + System.currentTimeMillis() + ";" + s + ";" + tr
                    + ";" + lote.size() + ";" + bytes);
        } catch (RuntimeException e) {
            log("ERRO;p" + k + ";lote: " + e);
        }
    }

    // ================================================================ SEGUIDOR (ponto 5)

    private void onLote(String msg) {
        if (Wire.isolated(k, i)) return;
        if (!msg.startsWith("LOTE")) return;
        Batch b;
        try {
            b = Batch.decode(msg, n);
        } catch (IllegalArgumentException e) {
            log("ERRO;p" + k + ";LOTE mal formado: " + e.getMessage());
            return;
        }
        if (b.getLeader() == i) return;                            // o próprio LOTE

        synchronized (this) {                                      // nó -> raft
            // termo inferior -> false (líder obsoleto); superior -> adota-o e demite-se se era líder
            if (!raft.onBatch(b.getTerm(), b.getLeader())) {
                log("IGNORADO;p" + k + ";LOTE termo=" + b.getTerm() + " lider=n" + b.getLeader()
                        + " (termo atual " + raft.term() + ")");
                return;
            }
            log("RECEBIDO;p" + k + ";termo=" + b.getTerm() + ";lider=n" + b.getLeader()
                    + ";hb=" + b.getHb() + ";ultimoSeq=" + b.getLastSeq() + ";ultimoTermo=" + b.getLastTerm()
                    + ";n=" + b.getRecords().size() + ";intervalo=" + b.seqRange());
            try {
                aplicarLote(b);
            } catch (IOException e) {
                log("ERRO;p" + k + ";aplicar LOTE: " + e.getMessage());
            }
        }
    }

    // chamado com o monitor do nó adquirido
    private void aplicarLote(Batch b) throws IOException {
        boolean divergencia = false;
        for (SensorRecord r : b.getRecords()) {
            if (r.getSeq() <= ultimoSeq) {
                SensorRecord meu = file.get(r.getSeq());
                if (meu != null && meu.getTerm() == r.getTerm()) {
                    continue;                                      // duplicado: o mesmo registo
                }
                divergencia = true;                                // mesmo seq, outro termo
                break;
            } else if (r.getSeq() == ultimoSeq + 1) {
                acrescentar(r);                                    // o seguinte: acrescenta
            } else {
                break;                                             // lacuna: recupera no fim do lote
            }
        }

        // ---- fim do lote: comparar o meu último registo com o do líder ----
        long ls = b.getLastSeq();
        long lt = b.getLastTerm();
        if (!divergencia) {
            if (ultimoSeq > ls) {
                divergencia = true;                                // tenho registos para além de ultimoSeq
            } else if (ultimoSeq == ls && ultimoTermo != lt) {
                divergencia = true;                                // o meu registo ultimoSeq tem outro termo
            } else if (ultimoSeq < ls && ultimoSeq > 0 && ultimoTermo != b.getTerm()) {
                // faltam-me registos e o meu último é de um termo anterior: não sei se o líder o tem;
                // a resolução por blocos confirma (sem truncar nada, se coincidir)
                divergencia = true;
            }
        }

        if (divergencia) {
            resolverDivergencia(b);
        } else if (ultimoSeq < ls) {
            recuperar(b, ultimoSeq + 1, ls);                       // lacuna ou fim do lote em falta
        }
    }

    private void acrescentar(SensorRecord r) throws IOException {
        file.append(r);
        ultimoSeq = r.getSeq();
        ultimoTermo = r.getTerm();
    }

    // lacuna simples: pede [de, ate] ao líder e acrescenta pela ordem
    private void recuperar(Batch b, long de, long ate) throws IOException {
        List<SensorRecord> recebidos = pedir(b, de, ate);
        if (recebidos == null) return;                             // falhou: tenta no lote seguinte
        int aplicados = 0;
        for (SensorRecord r : recebidos) {
            if (r.getSeq() != ultimoSeq + 1) break;
            acrescentar(r);
            aplicados++;
        }
        log("RECUPERACAO;p" + k + ";de=" + de + ";ate=" + ate + ";recebidos=" + aplicados);
    }

    /*
     * Divergência: o registo do líder é a referência.
     *  1. começa no primeiro seq do MEU último bloco de termo;
     *  2. pede ao líder [inicio, ultimoSeq do líder]; se o primeiro registo recebido não
     *     coincidir com o meu (mesmo seq e termo), recua um bloco e repete;
     *  3. encontrado o ponto em comum, o corte é o primeiro seq em que os dois diferem;
     *  4. trunca o ficheiro a partir do corte (atomicamente) e aplica os registos do líder.
     */
    private void resolverDivergencia(Batch b) throws IOException {
        List<SensorRecord> meus = file.all();
        if (meus.isEmpty()) {                                      // nada meu a comparar
            recuperar(b, 1, b.getLastSeq());
            return;
        }
        int idx = inicioBloco(meus, meus.size() - 1);             // índice do 1.º registo do último bloco
        List<SensorRecord> lider;
        while (true) {
            long inicio = meus.get(idx).getSeq();
            lider = pedir(b, inicio, Math.max(inicio, b.getLastSeq()));
            if (lider == null) return;                             // falhou: tenta no lote seguinte
            boolean coincide = !lider.isEmpty() && mesmoRegisto(lider.get(0), meus.get(idx));
            if (coincide || idx == 0) break;
            idx = inicioBloco(meus, idx - 1);                      // recua um bloco de termo
        }

        // ponto de corte: primeiro seq a partir de 'inicio' em que o meu e o do líder diferem
        Map<Long, SensorRecord> doLider = new HashMap<>();
        for (SensorRecord r : lider) doLider.put(r.getSeq(), r);
        long inicio = meus.get(idx).getSeq();
        long corte = -1;
        for (int j = idx; j < meus.size(); j++) {
            SensorRecord meu = meus.get(j);
            SensorRecord dele = doLider.get(meu.getSeq());
            if (dele == null || !mesmoRegisto(meu, dele)) {
                corte = meu.getSeq();
                break;
            }
        }

        if (corte != -1) {
            List<SensorRecord> descartados = file.truncateFrom(corte);
            TreeSet<Long> termos = new TreeSet<>();
            for (SensorRecord r : descartados) termos.add(r.getTerm());
            carregarUltimoRegisto();                               // ultimoSeq = corte - 1
            log("TRUNCADO;p" + k + ";de=" + corte + ";descartados=" + descartados.size()
                    + ";termos=" + termos);
        }

        // aplica os registos do líder que vêm a seguir ao meu último
        int aplicados = 0;
        for (SensorRecord r : lider) {
            if (r.getSeq() == ultimoSeq + 1) {
                acrescentar(r);
                aplicados++;
            }
        }
        if (corte == -1 && aplicados > 0) {
            log("RECUPERACAO;p" + k + ";de=" + inicio + ";ate=" + b.getLastSeq() + ";recebidos=" + aplicados);
        }
    }

    // índice do primeiro registo do bloco de termo que contém meus[j]
    private static int inicioBloco(List<SensorRecord> meus, int j) {
        long termo = meus.get(j).getTerm();
        while (j > 0 && meus.get(j - 1).getTerm() == termo) j--;
        return j;
    }

    // mesmo seq e mesmo termo = o mesmo registo
    private static boolean mesmoRegisto(SensorRecord a, SensorRecord b) {
        return a.getSeq() == b.getSeq() && a.getTerm() == b.getTerm();
    }

    // ================================================================ RECUPERAÇÃO TCP

    // cliente: "PEDIDO <de> <até>" ao líder do lote; devolve os registos ou null se falhar
    private List<SensorRecord> pedir(Batch b, long de, long ate) {
        if (Wire.isolated(k, i)) return null;                      // isolado: TCP falha
        List<SensorRecord> out = new ArrayList<>();
        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(b.getAddress(), b.getPort()), Wire.T_TCP_MS);
            s.setSoTimeout(Wire.T_TCP_MS);
            PrintWriter w = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);
            BufferedReader r = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
            w.println("PEDIDO " + de + " " + ate);
            String linha;
            while ((linha = r.readLine()) != null) {
                if (linha.equals("FIM")) return out;
                if (linha.startsWith("ERRO")) {
                    log("ERRO;p" + k + ";recuperação recusada: " + linha);
                    return null;
                }
                out.add(SensorRecord.fromLine(linha, n));
            }
            log("ERRO;p" + k + ";recuperação: ligação fechada antes de FIM");
        } catch (IOException | IllegalArgumentException e) {
            log("ERRO;p" + k + ";recuperação de n" + b.getLeader() + ": " + e.getMessage());
        }
        return null;
    }

    // servidor: serve registos do SEU ficheiro a qualquer nó, seja ou não líder
    private void iniciarServidorRecuperacao() throws IOException {
        ServerSocket server = new ServerSocket(Wire.nodePort(k, i));   // falha já se o porto estiver ocupado
        Thread t = new Thread(() -> {
            while (true) {
                try (Socket s = server.accept()) {
                    if (Wire.isolated(k, i)) continue;                 // isolado: fecha sem responder
                    s.setSoTimeout(Wire.T_TCP_MS);
                    BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                    PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);
                    atender(in.readLine(), out);
                } catch (IOException e) {
                    log("ERRO;p" + k + ";servidor TCP: " + e.getMessage());
                }
            }
        }, "recuperacao-p" + k + "-n" + i);
        t.setDaemon(true);
        t.start();
    }

    // "PEDIDO <de> <até>" -> linhas de registo + FIM, ou ERRO
    private void atender(String pedido, PrintWriter out) throws IOException {
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
        // lê do ficheiro (lock do RecordFile), sem o monitor do nó: não bloqueia os IN
        for (SensorRecord r : file.range(de, ate)) out.println(r.toLine());
        out.println("FIM");
    }

    // ================================================================ utilitários

    // devolve true se publicou
    private boolean publicar(String topic, String msg) {
        if (Wire.isolated(k, i)) return false;                     // isolado não publica
        try {
            ps.publish(topic, msg);
            return true;
        } catch (IOException e) {
            log("ERRO;p" + k + ";publicar em " + topic + ": " + e.getMessage());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }
}