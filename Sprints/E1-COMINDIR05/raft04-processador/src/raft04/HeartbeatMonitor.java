package raft04;

import java.util.function.Consumer;

public final class HeartbeatMonitor {
    public enum State { DESCONHECIDO, ATIVO, SUSPEITO }

    private final int partition;
    private final long timeoutNanos;
    private final Consumer<String> log;

    private State state = State.DESCONHECIDO;
    private long lastNanos;   // instante local da última receção
    
    // NOVAS VARIÁVEIS RAFT:
    private long term = -1;   // termo do líder atual
    private int leader = -1;  // ID do líder
    private long hb = 0;      // último heartbeat observado

    public HeartbeatMonitor(int partition, long periodMs, int factor, Consumer<String> log) {
        this.partition = partition;
        this.timeoutNanos = periodMs * factor * 1_000_000L;
        this.log = log;
        this.lastNanos = System.nanoTime();
    }

    // thread de receção: chegou um LOTE da partição
    public synchronized void onHeartbeat(long term, int leader, long hb) {
        if (term < this.term) return; // ignora termos inferiores (líder obsoleto)
        
        if (term > this.term) { // O termo aumentou
            log.accept("NOVO_LIDER;p" + partition + ";termo=" + term + ";lider=n" + leader);
            this.term = term;
            this.leader = leader;
            this.hb = hb;
        } else { // O termo é igual
            if (hb <= this.hb) return; // duplicado ou atrasado
            
            if (hb != this.hb + 1) { // Salto de hb dentro do mesmo termo
                log.accept("HB_PERDIDO;p" + partition + ";esperado=" + (this.hb + 1) + ";recebido=" + hb);
            }
            this.hb = hb;
        }

        if (state == State.SUSPEITO) {
            log.accept("RETOMA;p" + partition + ";termo=" + term + ";hb=" + hb);
        }
        
        this.lastNanos = System.nanoTime();
        this.state = State.ATIVO;
    }

    // thread de verificação: chamada a cada VERIFICACAO_MS
    public synchronized void check() {
        long elapsed = System.nanoTime() - lastNanos;
        if (state != State.SUSPEITO && elapsed > timeoutNanos) {
            state = State.SUSPEITO;
            log.accept("SUSPEITA;p" + partition + ";silencio_ms=" + elapsed / 1_000_000);
        }
    }

    public synchronized State state() {
        return state;
    }
}