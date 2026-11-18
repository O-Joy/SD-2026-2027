package raft04;

public class VoteRequest {
    public final long term;
    public final int candidate;
    public final long lastSeq;
    public final long lastTerm;

    public VoteRequest(long term, int candidate, long lastSeq, long lastTerm) {
        this.term = term;
        this.candidate = candidate;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
    }

    public String encode() {
        return "PEDIDO_VOTO;" + term + ";" + candidate + ";" + lastSeq + ";" + lastTerm;
    }

    public static VoteRequest decode(String msg) {
        String[] p = msg.split(";");
        return new VoteRequest(Long.parseLong(p[1]), Integer.parseInt(p[2]), 
                               Long.parseLong(p[3]), Long.parseLong(p[4]));
    }
}