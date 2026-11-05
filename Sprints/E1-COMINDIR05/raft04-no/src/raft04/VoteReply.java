package raft04;

public class VoteReply {
    public final long term;
    public final int voter;
    public final int candidate;
    public final boolean granted;

    public VoteReply(long term, int voter, int candidate, boolean granted) {
        this.term = term;
        this.voter = voter;
        this.candidate = candidate;
        this.granted = granted;
    }

    public String encode() {
        return "VOTO;" + term + ";" + voter + ";" + candidate + ";" + (granted ? "S" : "N");
    }

    public static VoteReply decode(String msg) {
        String[] p = msg.split(";");
        return new VoteReply(Long.parseLong(p[1]), Integer.parseInt(p[2]), 
                             Integer.parseInt(p[3]), p[4].equals("S"));
    }
}