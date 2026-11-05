package raft04;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class SensorRecord {
    // COMINDIR04: instante com milissegundos, para medir a latência de lote (formato ISO, sempre com .SSS)
    private static final DateTimeFormatter INSTANTE = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private final long seq;
    private final long term; // <-- NOVIDADE: o termo do Raft
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    private final long lamport;
    private final VectorClock vector;

    public SensorRecord(long seq, long term, String sensorId, double temperature, String timestamp, long lamport, VectorClock vector) {
        this.seq = seq;
        this.term = term;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector;
    }

    // cria um registo com o instante atual (usado só pelo líder)
    public static SensorRecord now(long seq, long term, String sensorId, double temperature, long lamport, VectorClock vector) {
        return new SensorRecord(seq, term, sensorId, temperature, LocalDateTime.now().format(INSTANTE), lamport, vector);
    }

    // instante do registo em milissegundos desde a época (mesma escala que System.currentTimeMillis()).
    public long getTimestampMillis() {
        return LocalDateTime.parse(timestamp).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    // representação textual: é o que se guarda no ficheiro e o que se envia pela rede
    public String toLine() {
        // Formato COMINDIR05: seq;termo;sensor;temperatura;instante;L;V
        return seq + ";" + term + ";" + sensorId + ";" + temperature + ";" + timestamp + ";" + lamport + ";" + vector;
    }

    public static SensorRecord fromLine(String line, int n) {
        String[] f = line.trim().split(";");
        if (f.length != 7) { // <-- Agora são 7 campos em vez de 6
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        return new SensorRecord(
                Long.parseLong(f[0]), // seq
                Long.parseLong(f[1]), // term
                f[2],                 // sensorId
                Double.parseDouble(f[3]), // temperature
                f[4],                 // timestamp
                Long.parseLong(f[5]), // lamport
                VectorClock.parse(f[6], n) // vector
        );
    }

    public long getSeq() { return seq; }
    public long getTerm() { return term; } // <-- Novo getter
    public String getSensorId() { return sensorId; }
    public double getTemperature() { return temperature; }
    public String getTimestamp() { return timestamp; }
    public long getLamport() { return lamport; }
    public VectorClock getVector() { return vector; }
}