package tpf;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Conjunto de registos selados de uma só vez.
 *
 * Formato binário (big-endian, escrito com DataOutputStream):
 *   int   n                         número de registos
 *   n vezes, um por registo:
 *     long  lsn
 *     int   comprimento + bytes UTF-8 do instante
 *     int   comprimento + bytes UTF-8 do autor
 *     int   comprimento + bytes UTF-8 do canal
 *     int   comprimento + bytes UTF-8 do texto
 *   bytes a zero (padding) até o total ser múltiplo de PAD_BYTES
 */
public final class Chunk {

    private final List<Registo> registos = new ArrayList<>();
    private int tamanho = 4;                 // bytes já ocupados, sem padding (começa com o campo n)

    public void adicionar(Registo r) {
        registos.add(r);
        tamanho += tamanhoDe(r);
    }

    public List<Registo> getRegistos() { return Collections.unmodifiableList(registos); }
    public boolean vazio()             { return registos.isEmpty(); }
    public int tamanho()               { return tamanho; }
    public long lsnDe()                { return registos.get(0).lsn(); }
    public long lsnAte()               { return registos.get(registos.size() - 1).lsn(); }

    // bytes que um registo ocupa no formato acima
    private static int tamanhoDe(Registo r) {
        return 8
                + 4 + utf8(r.instante()).length
                + 4 + utf8(r.autor()).length
                + 4 + utf8(r.canal()).length
                + 4 + utf8(r.texto()).length;
    }

    // registos em binário, com padding até um múltiplo de padBytes
    public byte[] serializar(int padBytes) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream(tamanho + padBytes);
            DataOutputStream out = new DataOutputStream(bos);
            out.writeInt(registos.size());
            for (Registo r : registos) {
                out.writeLong(r.lsn());
                escreverTexto(out, r.instante());
                escreverTexto(out, r.autor());
                escreverTexto(out, r.canal());
                escreverTexto(out, r.texto());
            }
            int resto = bos.size() % padBytes;
            if (resto != 0) out.write(new byte[padBytes - resto]);   // padding: zeros
            out.flush();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new UncheckedIOException(e);   // não acontece: escreve só em memória
        }
    }

    // operação inversa: lê os n registos e ignora o padding que vem depois
    public static Chunk ler(byte[] dados) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(dados));
        int n = in.readInt();
        if (n < 0) throw new IOException("chunk inválido: número de registos negativo");
        Chunk c = new Chunk();
        for (int i = 0; i < n; i++) {
            long lsn = in.readLong();
            String instante = lerTexto(in);
            String autor = lerTexto(in);
            String canal = lerTexto(in);
            String texto = lerTexto(in);
            c.adicionar(new Registo(lsn, instante, autor, canal, texto));
        }
        return c;
    }

    private static void escreverTexto(DataOutputStream out, String s) throws IOException {
        byte[] b = utf8(s);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String lerTexto(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0 || len > in.available()) throw new IOException("chunk inválido: comprimento " + len);
        byte[] b = new byte[len];
        in.readFully(b);
        return new String(b, StandardCharsets.UTF_8);
    }

    private static byte[] utf8(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}