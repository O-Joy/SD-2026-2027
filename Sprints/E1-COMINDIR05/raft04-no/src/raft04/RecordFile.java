package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.List;
import java.util.ArrayList;

public class RecordFile {
    private final Path path;
    private final int n;

    public RecordFile(String fileName, int n) {
        this.path = Paths.get(fileName);
        this.n = n;
    }

    // acrescenta uma linha ao fim do ficheiro (cria-o se não existir)
    public synchronized void append(SensorRecord r) throws IOException {
        Files.writeString(path, r.toLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    // número de sequência do último registo guardado (0 se o ficheiro não existir)
    public synchronized long lastSeq() throws IOException {
        if (!Files.exists(path)) {
            return 0;
        }
        long last = 0;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                last = SensorRecord.fromLine(line, n).getSeq();
            }
        }
        return last;
    }

    // registos com from <= seq <= to, pela ordem do ficheiro (lista vazia se o ficheiro não existir)
    public synchronized List<SensorRecord> range(long from, long to) throws IOException {
        List<SensorRecord> out = new ArrayList<>();
        if (!Files.exists(path)) {
            return out;
        }
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            SensorRecord r = SensorRecord.fromLine(line, n);
            if (r.getSeq() >= from && r.getSeq() <= to) {
                out.add(r);
            }
        }
        return out;
    }

    // COMINDIR05: todos os registos do ficheiro, pela ordem do seq
    public synchronized List<SensorRecord> all() throws IOException {
        return range(1, Long.MAX_VALUE);
    }

    // COMINDIR05: último registo do ficheiro, ou null se estiver vazio
    public synchronized SensorRecord last() throws IOException {
        List<SensorRecord> todos = all();
        return todos.isEmpty() ? null : todos.get(todos.size() - 1);
    }

    // COMINDIR05: registo com este seq, ou null se não existir
    public synchronized SensorRecord get(long seq) throws IOException {
        List<SensorRecord> r = range(seq, seq);
        return r.isEmpty() ? null : r.get(0);
    }

    // COMINDIR05: reescreve o ficheiro de forma ATÓMICA só com os registos de seq < from
    // (ficheiro temporário + ATOMIC_MOVE, como o estado do RaftState). Devolve os descartados.
    public synchronized List<SensorRecord> truncateFrom(long from) throws IOException {
        List<SensorRecord> manter = new ArrayList<>();
        List<SensorRecord> descartados = new ArrayList<>();
        for (SensorRecord r : all()) {
            if (r.getSeq() < from) manter.add(r); else descartados.add(r);
        }
        StringBuilder sb = new StringBuilder();
        for (SensorRecord r : manter) sb.append(r.toLine()).append(System.lineSeparator());
        Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
        Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8);
        Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        return descartados;
    }
}