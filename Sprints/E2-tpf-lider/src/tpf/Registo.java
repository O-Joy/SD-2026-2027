package tpf;

public record Registo(
    long lsn,
    String instante,
    String autor,
    String canal,
    String texto
) {}