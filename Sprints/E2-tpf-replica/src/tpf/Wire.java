package tpf;

/**
 * Parâmetros da réplica. Os valores são os mesmos do Wire do líder e alteram-se com -Dtpf.*
 * (o projeto tpf-replica é separado do tpf-lider, por isso tem o seu próprio Wire).
 */
public final class Wire {
    public static final long PERIODO_HB_MS  = Long.getLong("tpf.periodoHbMs", 2000L);
    public static final long BACKOFF_MIN_MS = Long.getLong("tpf.backoffMinMs", 1000L);
    public static final long BACKOFF_MAX_MS = Long.getLong("tpf.backoffMaxMs", 30000L);

    // tópicos
    public static final String TOPICO_LIDER = "tpf-lider";
    public static final String TOPICO_CTL   = "tpf-ctl";
    public static String topicoReplica(String id) { return "tpf-r-" + id; }

    private Wire() {}
}
