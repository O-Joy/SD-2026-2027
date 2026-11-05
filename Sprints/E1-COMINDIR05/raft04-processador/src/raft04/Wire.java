package raft04;

public final class Wire {
    public static final int LIMITE_RETIDOS = 10;
    public static final int T_TCP_MS = 5000;
    public static final double LIMIAR_ALARME = 28.0;

    // ---- RAFT, eleição e heartbeat (COMINDIR05) ----
    
    // período Δ entre lotes/heartbeats: -Draft04.periodo=<ms> (por omissão 2000)
    public static final long PERIODO_HB_MS = Long.getLong("raft04.periodo", 2000L);
    
    // TIMEOUT = FATOR_TIMEOUT × PERIODO_HB_MS: -Draft04.fator=<inteiro> (por omissão 3)
    public static final int FATOR_TIMEOUT = Integer.getInteger("raft04.fator", 3);
    
    // período de verificação da eleição (por omissão 200)
    public static final long VERIFICACAO_MS = 200;

    // Prazos de eleição
    public static final long ELEICAO_MIN_MS = Long.getLong("raft04.eleicaoMin", 3 * PERIODO_HB_MS);
    public static final long ELEICAO_MAX_MS = Long.getLong("raft04.eleicaoMax", 6 * PERIODO_HB_MS);
    
    // Número de nós por partição (G)
    public static final int GRUPO = 3;

    private Wire() { }

    // Tópicos IPFS atualizados para "raft04"
    public static String topicIn(int k)       { return "raft04-in-p" + k; }
    public static String topicData(int k)     { return "raft04-p" + k; }
    public static String topicElection(int k) { return "raft04-eleicao-p" + k; }

    // Porto TCP de recuperação do nó i da partição k
    public static int nodePort(int k, int i) {
        return 30000 + (k * 10) + i;
    }

    // Função que simula uma partição de rede (isola o nó se o ficheiro existir)
    public static boolean isolated(int k, int i) {
        return java.nio.file.Files.exists(java.nio.file.Path.of("isolar-p" + k + "-n" + i));
    }

    public static String in(String sensor, double temp, long lamport, VectorClock vector) { 
        return "IN;" + sensor + ";" + temp + ";" + lamport + ";" + vector; 
    }

    public static String type(String msg) {
        String m = msg.trim();
        int i = m.indexOf(';');
        if (i <= 0) {
            throw new IllegalArgumentException("Mensagem sem tipo: " + msg);
        }
        return m.substring(0, i);
    }

    public static String body(String msg) {
        String m = msg.trim();
        return m.substring(m.indexOf(';') + 1);
    }

    // O método regParts() da COMINDIR03 já pode ser removido, pois o sistema agora só usa LOTE e mensagens de eleição.
}