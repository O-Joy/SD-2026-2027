package tpf;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.HexFormat;

/**
 * Cifra e abertura de chunks.
 *
 *   salt  = 32 bytes aleatórios, novos em cada chunk
 *   nonce = 12 bytes aleatórios, novos em cada chunk
 *   DEK   = HKDF-SHA256(MK, salt, info = "tpf-chunk-v1"), 32 bytes:
 *             PRK = HMAC-SHA256(chave = salt, MK)                 (extração)
 *             DEK = HMAC-SHA256(chave = PRK, info || 0x01)        (expansão, um só bloco)
 *   aad   = "tpf-v1" || keyVersion (int de 4 bytes, big-endian) || salt || nonce
 *   texto cifrado = AES-256-GCM(DEK, nonce, aad, texto com padding), com a tag de 128 bits no fim
 *   chunkId = SHA-256(texto cifrado) em hexadecimal
 */
public final class ChunkCrypto {

    public static final int KEY_VERSION = 1;
    public static final int SALT_BYTES = 32;
    public static final int NONCE_BYTES = 12;
    public static final int TAG_BITS = 128;
    private static final byte[] INFO = "tpf-chunk-v1".getBytes(StandardCharsets.US_ASCII);
    private static final byte[] AAD_PREFIXO = "tpf-v1".getBytes(StandardCharsets.US_ASCII);

    private static final SecureRandom RANDOM = new SecureRandom();

    private ChunkCrypto() { }

    // cifra o texto com padding (Chunk.serializar) e devolve o chunk selado, com o seu chunkId
    public static Sealed seal(byte[] mk, byte[] texto) throws GeneralSecurityException {
        byte[] salt = new byte[SALT_BYTES];
        byte[] nonce = new byte[NONCE_BYTES];
        RANDOM.nextBytes(salt);
        RANDOM.nextBytes(nonce);

        Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
        c.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(dek(mk, salt), "AES"), new GCMParameterSpec(TAG_BITS, nonce));
        c.updateAAD(aad(KEY_VERSION, salt, nonce));
        byte[] ct = c.doFinal(texto);                      // inclui a tag nos últimos 16 bytes

        return new Sealed(sha256Hex(ct), KEY_VERSION, salt, nonce, ct);
    }

    /**
     * Abre um chunk selado e devolve o texto com padding. Verifica, por esta ordem:
     *   1. SHA-256 do texto cifrado == chunkId
     *   2. keyVersion conhecida
     *   3. autenticação GCM (falha se mudar um byte, o salt, o nonce, a keyVersion ou a master key)
     * Qualquer falha lança GeneralSecurityException.
     */
    public static byte[] open(byte[] mk, Sealed s) throws GeneralSecurityException {
        if (!MessageDigest.isEqual(sha256Hex(s.ciphertext()).getBytes(StandardCharsets.US_ASCII),
                s.chunkId().getBytes(StandardCharsets.US_ASCII))) {
            throw new GeneralSecurityException("hash não confere com o chunkId " + s.chunkId());
        }
        if (s.keyVersion() != KEY_VERSION) {
            throw new GeneralSecurityException("keyVersion desconhecida: " + s.keyVersion());
        }
        if (s.salt().length != SALT_BYTES || s.nonce().length != NONCE_BYTES) {
            throw new GeneralSecurityException("salt ou nonce com tamanho errado");
        }
        Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
        c.init(Cipher.DECRYPT_MODE, new SecretKeySpec(dek(mk, s.salt()), "AES"),
                new GCMParameterSpec(TAG_BITS, s.nonce()));
        c.updateAAD(aad(s.keyVersion(), s.salt(), s.nonce()));
        return c.doFinal(s.ciphertext());                  // AEADBadTagException se a tag não conferir
    }

    // HKDF-SHA256 com um único bloco de expansão: 32 bytes
    static byte[] dek(byte[] mk, byte[] salt) throws GeneralSecurityException {
        Mac mac = Mac.getInstance("HmacSHA256");
        mac.init(new SecretKeySpec(salt, "HmacSHA256"));
        byte[] prk = mac.doFinal(mk);                      // extração

        mac.init(new SecretKeySpec(prk, "HmacSHA256"));
        mac.update(INFO);
        mac.update((byte) 0x01);
        return mac.doFinal();                              // expansão: T(1)
    }

    static byte[] aad(int keyVersion, byte[] salt, byte[] nonce) {
        return ByteBuffer.allocate(AAD_PREFIXO.length + 4 + salt.length + nonce.length)
                .put(AAD_PREFIXO).putInt(keyVersion).put(salt).put(nonce).array();
    }

    static String sha256Hex(byte[] dados) throws GeneralSecurityException {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(dados));
    }
}
