package rep04;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Mensagem LOTE: heartbeat do líder e, ao mesmo tempo, lote com os registos do intervalo.
 *
 *   LOTE;<endereço>;<porto>;<inc>;<hb>;<ultimoSeq>;<n>
 *   <seq>;<sensor>;<temperatura>;<instante>;<L>;<V>      (n linhas, formato dos ficheiros)
 *
 * Um lote com n = 0 é um heartbeat puro (só a linha de cabeçalho).
 */
public final class Batch {
    private final String address;               // endereço anunciado pelo líder (recuperação TCP)
    private final int port;                     // porto do servidor de recuperação
    private final long inc;                     // incarnação: instante de arranque do líder (ms)
    private final long hb;                      // número do heartbeat, a partir de 1 em cada incarnação
    private final long lastSeq;                 // ultimoSeq no momento em que o lote foi fechado
    private final List<SensorRecord> records;   // registos do intervalo, pela ordem do seq

    public Batch(String address, int port, long inc, long hb, long lastSeq, List<SensorRecord> records) {
        this.address = address;
        this.port = port;
        this.inc = inc;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.records = Collections.unmodifiableList(new ArrayList<>(records));
    }

    // texto da mensagem LOTE: cabeçalho e uma linha por registo, separadas por '\n'
    public String encode() {
        StringBuilder sb = new StringBuilder();
        sb.append("LOTE;").append(address).append(';').append(port).append(';')
                .append(inc).append(';').append(hb).append(';').append(lastSeq).append(';').append(records.size());
        for (SensorRecord r : records) {
            sb.append('\n').append(r.toLine());
        }
        return sb.toString();
    }

    /**
     * Interpreta uma mensagem LOTE. O parâmetro n é o número de partições (dimensão dos vetores).
     * Lança IllegalArgumentException se o cabeçalho for inválido, se alguma linha de registo estiver
     * mal formada ou se o número de linhas não coincidir com o n anunciado no cabeçalho.
     */
    public static Batch decode(String msg, int n) {
        if (msg == null) {
            throw new IllegalArgumentException("LOTE vazio");
        }
        String[] linhas = msg.split("\\r?\\n");           // aceita \n e \r\n
        String[] h = linhas[0].trim().split(";");
        if (h.length != 7 || !h[0].equals("LOTE")) {
            throw new IllegalArgumentException("Cabeçalho de LOTE mal formado: " + linhas[0]);
        }
        String address = h[1];
        int port = Integer.parseInt(h[2]);                  // NumberFormatException é IllegalArgumentException
        long inc = Long.parseLong(h[3]);
        long hb = Long.parseLong(h[4]);
        long lastSeq = Long.parseLong(h[5]);
        int anunciados = Integer.parseInt(h[6]);
        if (anunciados < 0) {
            throw new IllegalArgumentException("Número de registos negativo: " + linhas[0]);
        }

        List<SensorRecord> records = new ArrayList<>();
        for (int i = 1; i < linhas.length; i++) {
            if (linhas[i].isBlank()) {
                continue;
            }
            records.add(SensorRecord.fromLine(linhas[i], n));
        }
        if (records.size() != anunciados) {
            throw new IllegalArgumentException("LOTE anuncia " + anunciados + " registos mas traz "
                    + records.size() + ": " + linhas[0]);
        }
        return new Batch(address, port, inc, hb, lastSeq, records);
    }

    public String getAddress() { return address; }
    public int getPort() { return port; }
    public long getInc() { return inc; }
    public long getHb() { return hb; }
    public long getLastSeq() { return lastSeq; }
    public List<SensorRecord> getRecords() { return records; }

    // intervalo de seq transportado, para o evento RECEBIDO ("-" num lote vazio)
    public String seqRange() {
        if (records.isEmpty()) {
            return "-";
        }
        return records.get(0).getSeq() + ".." + records.get(records.size() - 1).getSeq();
    }
}
