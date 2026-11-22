package tpf;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.Base64;

/**
 * Chunk selado: o texto cifrado e o cabeçalho necessário para o abrir.
 *
 * Em disco, na pasta selados/:
 *   <chunkId>.bin   texto cifrado AES-256-GCM, com a tag de 16 bytes no fim
 *   <chunkId>.hdr   uma linha de texto: keyVersion;salt;nonce
 *                   (keyVersion em decimal; salt e nonce em Base64)
 *
 * chunkId = SHA-256 do conteúdo do .bin, em hexadecimal minúsculo (64 carateres).
 */
public record Sealed(String chunkId, int keyVersion, byte[] salt, byte[] nonce, byte[] ciphertext) {

    // grava .bin e depois .hdr; cada um vai para um ficheiro temporário, com fsync, e só depois
    // é renomeado. Assim nunca fica em selados/ um ficheiro com o nome final mas incompleto.
    public void gravar(Path pasta) throws IOException {
        Files.createDirectories(pasta);
        gravarDuravel(pasta.resolve(chunkId + ".bin"), ciphertext);
        gravarDuravel(pasta.resolve(chunkId + ".hdr"), cabecalho().getBytes(StandardCharsets.UTF_8));
    }

    // lê de selados/ o .bin e o .hdr de um chunk (não verifica nada: isso é o ChunkCrypto.open)
    public static Sealed ler(Path pasta, String chunkId) throws IOException {
        byte[] ct = Files.readAllBytes(pasta.resolve(chunkId + ".bin"));
        String hdr = Files.readString(pasta.resolve(chunkId + ".hdr"), StandardCharsets.UTF_8).trim();
        String[] p = hdr.split(";");
        if (p.length != 3) {
            throw new IOException("cabeçalho mal formado: " + chunkId + ".hdr");
        }
        try {
            int keyVersion = Integer.parseInt(p[0]);
            byte[] salt = Base64.getDecoder().decode(p[1]);
            byte[] nonce = Base64.getDecoder().decode(p[2]);
            return new Sealed(chunkId, keyVersion, salt, nonce, ct);
        } catch (IllegalArgumentException e) {
            throw new IOException("cabeçalho mal formado: " + chunkId + ".hdr", e);
        }
    }

    // conteúdo do .hdr
    public String cabecalho() {
        Base64.Encoder b64 = Base64.getEncoder();
        return keyVersion + ";" + b64.encodeToString(salt) + ";" + b64.encodeToString(nonce) + "\n";
    }

    private static void gravarDuravel(Path destino, byte[] bytes) throws IOException {
        Path tmp = destino.resolveSibling(destino.getFileName() + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            ch.force(true);
        }
        Files.move(tmp, destino, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }
}
