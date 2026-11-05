package raft04;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Mensagem LOTE: heartbeat do líder e, ao mesmo tempo, lote com os registos do intervalo.
 *
 *   LOTE;<termo>;<lider>;<endereço>;<porto>;<hb>;<ultimoSeq>;<ultimoTermo>;<n>
 *   <seq>;<termo>;<sensor>;<temperatura>;<instante>;<L>;<V>      (n linhas, formato dos ficheiros)
 *
 * Um lote com n = 0 é um heartbeat puro (só a linha de cabeçalho).
 */
public final class Batch {
    private final long term;                // termo do líder atual
    private final int leader;               // identificador do nó líder
    private final String address;           // endereço anunciado pelo líder (recuperação TCP)
    private final int port;                 // porto do servidor de recuperação
    private final long hb;                  // número do heartbeat, a partir de 1 em cada termo
    private final long lastSeq;             // ultimoSeq no momento em que o lote foi fechado
    private final long lastTerm;            // termo do último registo (ultimoTermo)
    private final List<SensorRecord> records; // registos do intervalo, pela ordem do seq

    public Batch(long term, int leader, String address, int port, long hb, long lastSeq, long lastTerm, List<SensorRecord> records) {
        this.term = term;
        this.leader = leader;
        this.address = address;
        this.port = port;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    // texto da mensagem LOTE: cabeçalho e uma linha por registo, separadas por '\n'
    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOTE;").append(term).append(';').append(leader).append(';')
          .append(address).append(';').append(port).append(';')
          .append(hb).append(';').append(lastSeq).append(';').append(lastTerm).append(';')
          .append(records.size());
        
        for (SensorRecord r : records) {
            sb.append('\n').append(r.toLine());
        }
        return sb.toString();
    }

    /**
     * Interpreta uma mensagem LOTE. O parâmetro n_partitions é o número de partições (dimensão dos vetores).
     * Lança IllegalArgumentException se o cabeçalho for inválido, se alguma linha de registo estiver
     * mal formada ou se o número de linhas não coincidir com o n anunciado no cabeçalho.
     */
    public static Batch decode(String msg, int n_partitions) {
        if (msg == null) {
            throw new IllegalArgumentException("LOTE vazio");
        }
        String[] linhas = msg.split("\\r?\\n");           // aceita \n e \r\n
        String[] h = linhas[0].trim().split(";");
        
        if (h.length != 9 || !h[0].equals("LOTE")) {
            throw new IllegalArgumentException("Cabeçalho de LOTE mal formado: " + linhas[0]);
        }
        
        long term = Long.parseLong(h[1]);
        int leader = Integer.parseInt(h[2]);
        String address = h[3];
        int port = Integer.parseInt(h[4]);                  // NumberFormatException é IllegalArgumentException
        long hb = Long.parseLong(h[5]);
        long lastSeq = Long.parseLong(h[6]);
        long lastTerm = Long.parseLong(h[7]);
        int anunciados = Integer.parseInt(h[8]);
        
        if (anunciados < 0) {
            throw new IllegalArgumentException("Número de registos negativo: " + linhas[0]);
        }

        List<SensorRecord> records = new ArrayList<>();
        for (int i = 1; i < linhas.length; i++) {
            if (linhas[i].isBlank()) {
                continue;
            }
            records.add(SensorRecord.fromLine(linhas[i], n_partitions));
        }
        
        if (records.size() != anunciados) {
            throw new IllegalArgumentException("LOTE anuncia " + anunciados + " registos mas traz "
                    + records.size() + ": " + linhas[0]);
        }
        return new Batch(term, leader, address, port, hb, lastSeq, lastTerm, records);
    }

    public long getTerm() { return term; }
    public int getLeader() { return leader; }
    public String getAddress() { return address; }
    public int getPort() { return port; }
    public long getHb() { return hb; }
    public long getLastSeq() { return lastSeq; }
    public long getLastTerm() { return lastTerm; }
    public List<SensorRecord> getRecords() { return records; }

    // intervalo de seq transportado, para o evento RECEBIDO ("-" num lote vazio)
    public String seqRange() {
        if (records.isEmpty()) {
            return "-";
        }
        return records.get(0).getSeq() + ".." + records.get(records.size() - 1).getSeq();
    }
}