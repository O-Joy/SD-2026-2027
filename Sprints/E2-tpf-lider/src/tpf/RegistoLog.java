package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.Base64;
import java.util.List;

public class RegistoLog {
    private final Path logPath;
    private final Path eventosPath;
    private long ultimoLsn;

    public RegistoLog(Path logPath, Path eventosPath) throws IOException {
        this.logPath = logPath;
        this.eventosPath = eventosPath;
        this.ultimoLsn = recuperarUltimoLsn();
    }

    private long recuperarUltimoLsn() throws IOException {
        if (!Files.exists(logPath)) return 0L;
        List<String> linhas = Files.readAllLines(logPath, StandardCharsets.UTF_8);
        for (int i = linhas.size() - 1; i >= 0; i--) {
            String linha = linhas.get(i).trim();
            if (!linha.isEmpty()) {
                String[] partes = linha.split(";", 2);
                return Long.parseLong(partes[0]);
            }
        }
        return 0L;
    }

    public synchronized Registo registar(String autor, String canal, String texto) throws IOException {
        long lsn = ++ultimoLsn;
        String instante = Instant.now().toString();
        String textoB64 = Base64.getEncoder().encodeToString(texto.getBytes(StandardCharsets.UTF_8));

        String linha = lsn + ";" + instante + ";" + autor + ";" + canal + ";" + textoB64 + "\n";
        escreverComFsync(linha);
        registarEvento(instante, "REGISTO", "lsn=" + lsn + " autor=" + autor + " canal=" + canal);

        return new Registo(lsn, instante, autor, canal, texto);
    }

    private void escreverComFsync(String linha) throws IOException {
        byte[] bytes = linha.getBytes(StandardCharsets.UTF_8);
        try (FileChannel ch = FileChannel.open(
                logPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND)) {
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            ch.force(true); // fsync: garante escrita no disco antes de continuar
        }
    }

    public synchronized void registarEvento(String instante, String evento, String detalhe) throws IOException {
        String linha = instante + ";" + evento + ";" + detalhe + "\n";
        Files.writeString(
            eventosPath,
            linha,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND
        );
    }

    public synchronized long getUltimoLsn() {
        return ultimoLsn;
    }
}