package tpf;

public final class Wire {
    public static final int CHUNK_MAX_BYTES = Integer.getInteger("tpf.chunkMaxBytes", 65536);
    public static final int PAD_BYTES       = Integer.getInteger("tpf.padBytes", 4096);
    public static final long SELAGEM_MS     = Long.getLong("tpf.selagemMs", 5000L);

    // tamanho (aprox.) do texto de cada registo sintético do /rajada: com 1000 bytes cabem ~60 registos
    // num chunk de 64 KiB, por isso /rajada 500 0 sela vários chunks por tamanho
    public static final int RAJADA_TEXTO_BYTES = Integer.getInteger("tpf.rajadaTextoBytes", 1000);

    public static final int MASTER_KEY_BYTES = 32;

    // --- Sprint 3 ---
    public static final int PORTO              = Integer.getInteger("tpf.porto", 8765);
    public static final long PERIODO_HB_MS     = Long.getLong("tpf.periodoHbMs", 2000L);
    public static final long SUSPEITA_MS       = Long.getLong("tpf.suspeitaMs", 6000L);
    public static final long MORTA_MS          = Long.getLong("tpf.mortaMs", 10000L);
    public static final long TOKEN_VALIDADE_MS = Long.getLong("tpf.tokenValidadeMs", 600000L);

    public static final int R                  = Integer.getInteger("tpf.r", 3);
    public static final int W_MIN              = Integer.getInteger("tpf.wMin", 2);
    public static final long ACK_TIMEOUT_MS    = Long.getLong("tpf.ackTimeoutMs", 4000L);
    public static final int MAX_RONDAS         = Integer.getInteger("tpf.maxRondas", 3);

    public static final long MARGEM_BYTES      = Long.getLong("tpf.margemBytes", 65536L);
    public static final double ALFA            = Double.parseDouble(System.getProperty("tpf.alfa", "1.0"));

    public static final long BACKOFF_MIN_MS    = Long.getLong("tpf.backoffMinMs", 1000L);
    public static final long BACKOFF_MAX_MS    = Long.getLong("tpf.backoffMaxMs", 30000L);

    private Wire() {}

    /**
     * Número mínimo de ACKs válidos para um chunk ser DURAVEL:
     * max(W_MIN, floor(R / 2) + 1)
     */
    public static int minAcksParaDuravel(int r) {
        int maioria = (r / 2) + 1;
        return Math.max(W_MIN, maioria);
    }
}