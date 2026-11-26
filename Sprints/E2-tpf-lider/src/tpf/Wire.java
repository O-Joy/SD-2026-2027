package tpf;

public final class Wire {
    public static final int CHUNK_MAX_BYTES = Integer.getInteger("tpf.chunkMaxBytes", 65536);
    public static final int PAD_BYTES       = Integer.getInteger("tpf.padBytes", 4096);
    public static final long SELAGEM_MS     = Long.getLong("tpf.selagemMs", 5000L);

    // tamanho (aprox.) do texto de cada registo sintético do /rajada: com 1000 bytes cabem ~60 registos
    // num chunk de 64 KiB, por isso /rajada 500 0 sela vários chunks por tamanho
    public static final int RAJADA_TEXTO_BYTES = Integer.getInteger("tpf.rajadaTextoBytes", 1000);

    public static final int MASTER_KEY_BYTES = 32;

    private Wire() {}
}