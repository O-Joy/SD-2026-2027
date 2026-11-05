package rep04;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class SensorRecord {
    // COMINDIR04: instante com milissegundos, para medir a latência de lote (formato ISO, sempre com .SSS)
    private static final DateTimeFormatter INSTANTE = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");

    private final long seq;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    private final long lamport;
    private final VectorClock vector;

    public SensorRecord(long seq, String sensorId, double temperature, String timestamp, long lamport, VectorClock vector) {
        this.seq = seq;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector;
    }

    // cria um registo com o instante atual (usado só pelo líder)
    public static SensorRecord now(long seq, String sensorId, double temperature, long lamport, VectorClock vector) {
        return new SensorRecord(seq, sensorId, temperature, LocalDateTime.now().format(INSTANTE), lamport, vector);
    }

    // instante do registo em milissegundos desde a época (mesma escala que System.currentTimeMillis()).
    // Aceita também o formato da COMINDIR03, sem milissegundos.
    public long getTimestampMillis() {
        return LocalDateTime.parse(timestamp).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    // representação textual: é o que se guarda no ficheiro e o que se envia pela rede
    public String toLine() {
        return seq + ";" + sensorId + ";" + temperature + ";" + timestamp + ";" + lamport + ";" + vector;
    }

    public static SensorRecord fromLine(String line, int n) {
        String[] f = line.trim().split(";");
        if (f.length != 6) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        return new SensorRecord(Long.parseLong(f[0]), f[1], Double.parseDouble(f[2]), f[3], Long.parseLong(f[4]), VectorClock.parse(f[5], n));
    }

    public long getSeq() { return seq; }
    public String getSensorId() { return sensorId; }
    public double getTemperature() { return temperature; }
    public String getTimestamp() { return timestamp; }
    public long getLamport() { return lamport; }
    public VectorClock getVector() { return vector; }
}
