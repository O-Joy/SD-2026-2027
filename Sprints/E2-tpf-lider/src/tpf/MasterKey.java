package tpf;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.SecureRandom;
import java.util.Set;

public final class MasterKey {

    private MasterKey() {}

    public static byte[] carregarOuGerar(Path caminho) throws IOException {
        if (Files.exists(caminho)) {
            byte[] key = Files.readAllBytes(caminho);
            if (key.length != Wire.MASTER_KEY_BYTES) {
                throw new IllegalStateException(
                    "master.key inválida: esperados " + Wire.MASTER_KEY_BYTES +
                    " bytes, encontrados " + key.length
                );
            }
            return key;
        }

        byte[] key = new byte[Wire.MASTER_KEY_BYTES];
        new SecureRandom().nextBytes(key);
        Files.write(caminho, key, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        aplicarPermissoesSoDono(caminho);
        return key;
    }

    private static void aplicarPermissoesSoDono(Path caminho) throws IOException {
        try {
            Set<PosixFilePermission> perms = PosixFilePermissions.fromString("rw-------");
            Files.setPosixFilePermissions(caminho, perms);
        } catch (UnsupportedOperationException e) {
            // Fallback caso estejas em Windows
            File f = caminho.toFile();
            f.setReadable(false, false);
            f.setWritable(false, false);
            f.setExecutable(false, false);
            f.setReadable(true, true);
            f.setWritable(true, true);
        }
    }
}