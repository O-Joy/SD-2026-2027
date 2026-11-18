package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * CausalCheck da COMINDIR03, adaptado à COMINDIR05.
 *
 *   CausalCheck <N> <pasta>
 *
 * Diferenças em relação à versão da 03/04:
 *  - lê os ficheiros dos nós, no-p<k>-n<i>.txt (3 por partição), em vez de lider-p<k>.txt;
 *  - cada linha tem 7 campos: seq;termo;sensor;temperatura;instante;L;V;
 *  - verifica V[k] = seq e L/V crescentes em TODOS os ficheiros de nó, através das mudanças de termo
 *    (é o que prova que o novo líder continuou os relógios a partir do último registo);
 *  - para a análise de pares entre partições usa, em cada partição, o ficheiro de referência:
 *    o nó com mais registos (o mais completo; no fim de um ensaio, os três devem coincidir).
 */
public final class CausalCheck {

    private static final Pattern NO = Pattern.compile("no-p(\\d+)-n(\\d+)\\.txt");

    private static int erros = 0;
    private static int atencoes = 0;

    // um evento = um registo, com a partição onde foi escrito
    private static final class Evento {
        final int k;
        final SensorRecord r;

        Evento(int k, SensorRecord r) {
            this.k = k;
            this.r = r;
        }
    }

    private CausalCheck() { }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: CausalCheck <N> <pasta>");
            System.exit(2);
        }
        int n;
        try {
            n = Integer.parseInt(args[0]);
            if (n < 1) throw new NumberFormatException();
        } catch (NumberFormatException e) {
            System.out.println("N tem de ser um inteiro positivo");
            System.exit(2);
            return;
        }
        Path pasta = Paths.get(args[1]);
        if (!Files.isDirectory(pasta)) {
            System.out.println("Pasta inexistente: " + pasta);
            System.exit(2);
        }

        // --- inventário: partição -> (nó -> ficheiro) ---
        TreeMap<Integer, TreeMap<Integer, Path>> nos = new TreeMap<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(pasta)) {
            for (Path p : ds) {
                Matcher m = NO.matcher(p.getFileName().toString());
                if (m.matches()) {
                    nos.computeIfAbsent(Integer.parseInt(m.group(1)), x -> new TreeMap<>())
                            .put(Integer.parseInt(m.group(2)), p);
                }
            }
        }

        System.out.println("=== CausalCheck (COMINDIR05): N=" + n + ", pasta=" + pasta.toAbsolutePath() + " ===");
        for (int k : nos.keySet()) {
            if (k >= n) {
                erro("existem ficheiros no-p" + k + "-n*.txt, que não existem com N=" + n);
            }
        }

        // --- 1. V[k] = seq e crescimento de L e V, em cada ficheiro de nó ---
        System.out.println();
        System.out.println("--- 1. Equivalência V[k] = seq em todos os nós (através das mudanças de termo) ---");
        List<List<Evento>> referencia = new ArrayList<>();       // ficheiro de referência de cada partição
        List<Evento> todos = new ArrayList<>();
        for (int k = 0; k < n; k++) {
            TreeMap<Integer, Path> ficheiros = nos.getOrDefault(k, new TreeMap<>());
            if (ficheiros.isEmpty()) {
                atencao("p" + k + ": não há ficheiros no-p" + k + "-n*.txt (partição sem registos)");
                referencia.add(new ArrayList<>());
                continue;
            }
            List<Evento> melhor = new ArrayList<>();
            String melhorNome = "";
            for (Path f : ficheiros.values()) {
                List<Evento> evs = verificarFicheiro(k, f, n);
                if (evs.size() > melhor.size()) {
                    melhor = evs;
                    melhorNome = f.getFileName().toString();
                }
            }
            referencia.add(melhor);
            todos.addAll(melhor);
            System.out.println("  p" + k + ": ficheiro de referência para os pares = " + melhorNome
                    + " (" + melhor.size() + " registos)");
        }

        // --- 2. registos ---
        System.out.println();
        System.out.println("--- 2. Registos (ficheiros de referência) ---");
        StringBuilder sb = new StringBuilder("  registos por partição:");
        for (int k = 0; k < n; k++) {
            sb.append(" p").append(k).append("=").append(referencia.get(k).size());
        }
        System.out.println(sb + "  (total " + todos.size() + ")");
        long alarmes = todos.stream().filter(e -> e.r.getSensorId().startsWith("ALARM-")).count();
        System.out.println("  registos de alarmes (sensor ALARM-*): " + alarmes);

        // --- 3. classificação dos pares de partições diferentes ---
        long pares = 0, ab = 0, ba = 0, conc = 0, iguais = 0;
        long concLDistinto = 0, concLIgual = 0, violLamport = 0;
        List<String> exemplosViolacao = new ArrayList<>();
        for (int i = 0; i < todos.size(); i++) {
            Evento a = todos.get(i);
            for (int j = i + 1; j < todos.size(); j++) {
                Evento b = todos.get(j);
                if (a.k == b.k) continue;                          // só pares de partições diferentes
                pares++;
                switch (a.r.getVector().compare(b.r.getVector())) {
                    case BEFORE:
                        ab++;
                        violLamport += verificarLamport(a, b, exemplosViolacao);
                        break;
                    case AFTER:
                        ba++;
                        violLamport += verificarLamport(b, a, exemplosViolacao);
                        break;
                    case CONCURRENT:
                        conc++;
                        if (a.r.getLamport() != b.r.getLamport()) concLDistinto++; else concLIgual++;
                        break;
                    default:
                        iguais++;
                        break;
                }
            }
        }
        System.out.println();
        System.out.println("--- 3. Pares de registos de partições diferentes ---");
        System.out.println("  total de pares:            " + pares);
        System.out.println("  causalmente relacionados:  " + (ab + ba) + "  (a->b " + ab + ", b->a " + ba + ")");
        System.out.println("  concorrentes:              " + conc);
        if (iguais > 0) {
            erro(iguais + " par(es) de partições diferentes com vetores iguais");
        } else {
            ok("nenhum par de partições diferentes com vetores iguais");
        }

        System.out.println();
        System.out.println("--- 4. Condição de relógio de Lamport (a -> b  =>  L(a) < L(b)) ---");
        if (violLamport == 0) {
            ok("condição verificada em todos os " + (ab + ba) + " pares causais");
        } else {
            erro("condição violada em " + violLamport + " de " + (ab + ba) + " pares causais");
            for (String s : exemplosViolacao) System.out.println("    " + s);
        }

        System.out.println();
        System.out.println("--- 5. Pares concorrentes e o relógio de Lamport ---");
        System.out.println("  com L distinto: " + concLDistinto + "  (" + pct(concLDistinto, conc) + ")");
        System.out.println("  com L igual:    " + concLIgual + "  (" + pct(concLIgual, conc) + ")");

        // --- vista de Lamport ---
        todos.sort(Comparator.<Evento>comparingLong(e -> e.r.getLamport())
                .thenComparingInt(e -> e.k)
                .thenComparingLong(e -> e.r.getSeq()));
        List<String> vista = new ArrayList<>();
        for (Evento e : todos) {
            vista.add("L=" + e.r.getLamport() + " p" + e.k + "#" + e.r.getSeq() + " termo=" + e.r.getTerm() + " "
                    + e.r.getSensorId() + " " + e.r.getTemperature() + " V=" + e.r.getVector() + " " + e.r.getTimestamp());
        }
        Path saida = pasta.resolve("vista-lamport.txt");
        Files.write(saida, vista, StandardCharsets.UTF_8);
        System.out.println();
        System.out.println("vista-lamport.txt: " + vista.size() + " registos ordenados por (L, k)");

        System.out.println();
        System.out.println("RESULTADO: " + erros + " erro(s), " + atencoes + " atenção(ões)"
                + (erros == 0 ? " -> RELÓGIOS CONSISTENTES" : " -> FALHAS DETETADAS"));
        System.exit(erros == 0 ? 0 : 1);
    }

    // verifica um ficheiro de nó: V[k] = seq e L, V estritamente crescentes; devolve os registos
    private static List<Evento> verificarFicheiro(int k, Path f, int n) throws IOException {
        List<Evento> evs = new ArrayList<>();
        int invalidas = 0, viol = 0, locais = 0, mudancasTermo = 0;
        long lAnt = Long.MIN_VALUE, termoAnt = -1;
        VectorClock vAnt = null;
        List<String> linhas = Files.readAllLines(f, StandardCharsets.UTF_8);
        for (int i = 0; i < linhas.size(); i++) {
            String linha = linhas.get(i);
            if (linha.isBlank()) continue;
            SensorRecord r;
            try {
                r = SensorRecord.fromLine(linha, n);
            } catch (IllegalArgumentException e) {
                invalidas++;
                if (invalidas == 1) erro(f.getFileName() + ": linha " + (i + 1) + " mal formada (" + e.getMessage() + ")");
                continue;
            }
            evs.add(new Evento(k, r));
            if (r.getVector().get(k) != r.getSeq()) {
                viol++;
                if (viol <= 3) {
                    System.out.println("    " + f.getFileName() + " seq " + r.getSeq() + " (termo " + r.getTerm()
                            + "): V=" + r.getVector() + " → V[" + k + "]=" + r.getVector().get(k) + " != seq");
                }
            }
            if (termoAnt != -1 && r.getTerm() != termoAnt) mudancasTermo++;
            if (vAnt != null) {
                VectorClock.Order o = vAnt.compare(r.getVector());
                if (o != VectorClock.Order.BEFORE || r.getLamport() <= lAnt) {
                    locais++;
                    if (locais <= 3) {
                        System.out.println("    " + f.getFileName() + " seq " + r.getSeq() + ": não é posterior ao anterior (L "
                                + lAnt + " → " + r.getLamport() + ", V " + vAnt + " → " + r.getVector() + ")");
                    }
                }
            }
            lAnt = r.getLamport();
            vAnt = r.getVector();
            termoAnt = r.getTerm();
        }
        String nome = f.getFileName().toString();
        if (viol == 0) {
            ok(nome + ": " + evs.size() + " registos, " + mudancasTermo + " mudança(s) de termo, V[" + k + "] = seq em todas as linhas");
        } else {
            erro(nome + ": V[" + k + "] != seq em " + viol + " de " + evs.size() + " linhas");
        }
        if (locais > 0) {
            erro(nome + ": " + locais + " registo(s) não posterior(es) ao anterior (L ou V não crescem)");
        }
        return evs;
    }

    private static int verificarLamport(Evento a, Evento b, List<String> exemplos) {
        if (a.r.getLamport() < b.r.getLamport()) return 0;
        if (exemplos.size() < 5) {
            exemplos.add("p" + a.k + "#" + a.r.getSeq() + " (L=" + a.r.getLamport() + ") -> p" + b.k + "#"
                    + b.r.getSeq() + " (L=" + b.r.getLamport() + ")");
        }
        return 1;
    }

    private static String pct(long parte, long total) {
        return total == 0 ? "n/a" : String.format("%.1f%%", 100.0 * parte / total);
    }

    private static void ok(String s)      { System.out.println("  [OK]      " + s); }
    private static void atencao(String s) { atencoes++; System.out.println("  [ATENÇÃO] " + s); }
    private static void erro(String s)    { erros++; System.out.println("  [ERRO]    " + s); }
}