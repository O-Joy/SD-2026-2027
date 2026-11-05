package rep04;

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
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public final class CausalCheck {

    private static final Pattern LIDER = Pattern.compile("lider-p(\\d+)\\.txt");

    private static int erros = 0;
    private static int atencoes = 0;

    // um evento = um registo de um líder, com a partição onde foi escrito
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
            if (n < 1) {
                throw new NumberFormatException();
            }
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

        // --- inventário dos ficheiros dos líderes ---
        TreeMap<Integer, Path> lideres = new TreeMap<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(pasta)) {
            for (Path p : ds) {
                Matcher m = LIDER.matcher(p.getFileName().toString());
                if (m.matches()) {
                    lideres.put(Integer.parseInt(m.group(1)), p);
                }
            }
        }

        System.out.println("=== CausalCheck: N=" + n + ", pasta=" + pasta.toAbsolutePath() + " ===");
        for (int k : new TreeSet<>(lideres.keySet())) {
            if (k >= n) {
                erro("existe lider-p" + k + ".txt, que não existe com N=" + n + " (processos com N diferente?)");
            }
        }

        // --- leitura e verificação V[k] = seq ---
        List<List<Evento>> porParticao = new ArrayList<>();
        List<Evento> todos = new ArrayList<>();
        System.out.println();
        System.out.println("--- 1. Equivalência V[k] = seq ---");
        for (int k = 0; k < n; k++) {
            List<Evento> evs = new ArrayList<>();
            porParticao.add(evs);
            Path f = lideres.get(k);
            if (f == null) {
                atencao("falta lider-p" + k + ".txt (partição sem registos)");
                continue;
            }
            int invalidas = 0;
            int viol = 0;
            long lAnt = Long.MIN_VALUE;
            VectorClock vAnt = null;
            int locais = 0;
            List<String> linhas = Files.readAllLines(f, StandardCharsets.UTF_8);
            for (int i = 0; i < linhas.size(); i++) {
                String linha = linhas.get(i);
                if (linha.isBlank()) {
                    continue;
                }
                SensorRecord r;
                try {
                    r = SensorRecord.fromLine(linha, n);
                } catch (IllegalArgumentException e) {       // inclui NumberFormatException e dimensão errada
                    invalidas++;
                    if (invalidas == 1) {
                        erro(f.getFileName() + ": linha " + (i + 1) + " mal formada (" + e.getMessage() + ")");
                    }
                    continue;
                }
                evs.add(new Evento(k, r));
                todos.add(new Evento(k, r));
                if (r.getVector().get(k) != r.getSeq()) {
                    viol++;
                    if (viol <= 3) {
                        System.out.println("    seq " + r.getSeq() + ": V=" + r.getVector() + " → V[" + k + "]="
                                + r.getVector().get(k) + " != seq");
                    }
                }
                // coerência local: dentro de uma partição, cada evento é posterior ao anterior
                if (vAnt != null) {
                    VectorClock.Order o = vAnt.compare(r.getVector());
                    if (o != VectorClock.Order.BEFORE || r.getLamport() <= lAnt) {
                        locais++;
                        if (locais <= 3) {
                            System.out.println("    seq " + r.getSeq() + ": não é posterior ao anterior (L " + lAnt
                                    + " → " + r.getLamport() + ", V " + vAnt + " → " + r.getVector() + ")");
                        }
                    }
                }
                lAnt = r.getLamport();
                vAnt = r.getVector();
            }
            if (viol == 0) {
                ok(f.getFileName() + ": " + evs.size() + " registos, V[" + k + "] = seq em todas as linhas");
            } else {
                erro(f.getFileName() + ": V[" + k + "] != seq em " + viol + " de " + evs.size() + " linhas");
            }
            if (locais == 0) {
                ok(f.getFileName() + ": L estritamente crescente e V estritamente crescente dentro da partição");
            } else {
                erro(f.getFileName() + ": " + locais + " registo(s) não posterior(es) ao anterior (L ou V não crescem)");
            }
        }

        // --- registos por partição e alarmes ---
        System.out.println();
        System.out.println("--- 2. Registos ---");
        StringBuilder sb = new StringBuilder("  registos por partição:");
        for (int k = 0; k < n; k++) {
            sb.append(" p").append(k).append("=").append(porParticao.get(k).size());
        }
        System.out.println(sb + "  (total " + todos.size() + ")");
        long alarmes = todos.stream().filter(e -> e.r.getSensorId().startsWith("ALARM-")).count();
        System.out.println("  registos de alarmes (sensor ALARM-*): " + alarmes);

        // --- classificação dos pares de partições diferentes ---
        long pares = 0, ab = 0, ba = 0, conc = 0, iguais = 0;
        long concLDistinto = 0, concLIgual = 0;
        long violLamport = 0;
        long[][] causaisEntre = new long[n][n];
        long[][] concEntre = new long[n][n];
        List<String> exemplosViolacao = new ArrayList<>();
        for (int i = 0; i < todos.size(); i++) {
            Evento a = todos.get(i);
            for (int j = i + 1; j < todos.size(); j++) {
                Evento b = todos.get(j);
                if (a.k == b.k) {
                    continue;                                  // só pares de partições diferentes
                }
                pares++;
                int p = Math.min(a.k, b.k);
                int q = Math.max(a.k, b.k);
                VectorClock.Order o = a.r.getVector().compare(b.r.getVector());
                switch (o) {
                    case BEFORE:                               // V(a) < V(b)  =>  a -> b
                        ab++;
                        causaisEntre[p][q]++;
                        violLamport += verificarLamport(a, b, exemplosViolacao);
                        break;
                    case AFTER:                                // V(b) < V(a)  =>  b -> a
                        ba++;
                        causaisEntre[p][q]++;
                        violLamport += verificarLamport(b, a, exemplosViolacao);
                        break;
                    case CONCURRENT:
                        conc++;
                        concEntre[p][q]++;
                        if (a.r.getLamport() != b.r.getLamport()) {
                            concLDistinto++;
                        } else {
                            concLIgual++;
                        }
                        break;
                    default:                                   // EQUAL: impossível entre partições diferentes
                        iguais++;
                        if (iguais <= 3) {
                            System.out.println("    vetores iguais: p" + a.k + "#" + a.r.getSeq() + " e p" + b.k + "#"
                                    + b.r.getSeq() + " (V=" + a.r.getVector() + ")");
                        }
                        break;
                }
            }
        }

        System.out.println();
        System.out.println("--- 3. Pares de registos de partições diferentes ---");
        System.out.println("  total de pares:                 " + pares);
        System.out.println("  a -> b  (V(a) < V(b)):          " + ab);
        System.out.println("  b -> a  (V(b) < V(a)):          " + ba);
        System.out.println("  causalmente relacionados:       " + (ab + ba));
        System.out.println("  concorrentes:                   " + conc);
        for (int p = 0; p < n; p++) {
            for (int q = p + 1; q < n; q++) {
                System.out.println("    p" + p + " x p" + q + ": causais=" + causaisEntre[p][q]
                        + " concorrentes=" + concEntre[p][q]);
            }
        }
        if (iguais > 0) {
            erro(iguais + " par(es) de partições diferentes com vetores iguais (impossível num sistema correto)");
        } else {
            ok("nenhum par de partições diferentes com vetores iguais");
        }

        System.out.println();
        System.out.println("--- 4. Condição de relógio de Lamport (a -> b  =>  L(a) < L(b)) ---");
        if (violLamport == 0) {
            ok("condição verificada em todos os " + (ab + ba) + " pares causais");
        } else {
            erro("condição violada em " + violLamport + " de " + (ab + ba) + " pares causais");
            for (String s : exemplosViolacao) {
                System.out.println("    " + s);
            }
        }

        System.out.println();
        System.out.println("--- 5. Pares concorrentes e o relógio de Lamport ---");
        System.out.println("  pares concorrentes:             " + conc);
        System.out.println("  com L distinto:                 " + concLDistinto + "  (" + pct(concLDistinto, conc) + ")");
        System.out.println("  com L igual:                    " + concLIgual + "  (" + pct(concLIgual, conc) + ")");

        // --- tamanho estimado das mensagens REG ---
        System.out.println();
        System.out.println("--- 6. Tamanho médio estimado de uma mensagem REG (bytes) ---");
        long somaCom = 0, somaSem = 0;
        for (Evento e : todos) {
            String prefixo = "REG;127.0.0.1;" + (Wire.PORTO_BASE + e.k) + ";";
            String com = prefixo + e.r.toLine();
            String sem = prefixo + e.r.getSeq() + ";" + e.r.getSensorId() + ";" + e.r.getTemperature() + ";"
                    + e.r.getTimestamp();
            somaCom += com.getBytes(StandardCharsets.UTF_8).length;
            somaSem += sem.getBytes(StandardCharsets.UTF_8).length;
        }
        if (todos.isEmpty()) {
            System.out.println("  (sem registos)");
        } else {
            double mCom = (double) somaCom / todos.size();
            double mSem = (double) somaSem / todos.size();
            System.out.printf("  sem carimbos: %.1f   com carimbos: %.1f   diferença: %.1f%n", mSem, mCom, mCom - mSem);
            System.out.println("  (estimado a partir dos ficheiros, com endereço 127.0.0.1 e porto 7000+k)");
        }

        // --- vista de Lamport ---
        todos.sort(Comparator.<Evento>comparingLong(e -> e.r.getLamport())
                .thenComparingInt(e -> e.k)
                .thenComparingLong(e -> e.r.getSeq()));
        List<String> vista = new ArrayList<>();
        for (Evento e : todos) {
            vista.add("L=" + e.r.getLamport() + " p" + e.k + "#" + e.r.getSeq() + " " + e.r.getSensorId() + " "
                    + e.r.getTemperature() + " V=" + e.r.getVector() + " " + e.r.getTimestamp());
        }
        Path saida = pasta.resolve("vista-lamport.txt");
        Files.write(saida, vista, StandardCharsets.UTF_8);
        System.out.println();
        System.out.println("vista-lamport.txt: " + vista.size() + " registos ordenados por (L, k) → " + saida.toAbsolutePath());

        System.out.println();
        System.out.println("RESULTADO: " + erros + " erro(s), " + atencoes + " atenção(ões)"
                + (erros == 0 ? " -> RELÓGIOS CONSISTENTES" : " -> FALHAS DETETADAS"));
        System.exit(erros == 0 ? 0 : 1);
    }

    // a -> b: exige L(a) < L(b); devolve 1 se violar
    private static int verificarLamport(Evento a, Evento b, List<String> exemplos) {
        if (a.r.getLamport() < b.r.getLamport()) {
            return 0;
        }
        if (exemplos.size() < 5) {
            exemplos.add("p" + a.k + "#" + a.r.getSeq() + " (L=" + a.r.getLamport() + ", V=" + a.r.getVector()
                    + ") -> p" + b.k + "#" + b.r.getSeq() + " (L=" + b.r.getLamport() + ", V=" + b.r.getVector() + ")");
        }
        return 1;
    }

    private static String pct(long parte, long total) {
        return total == 0 ? "n/a" : String.format("%.1f%%", 100.0 * parte / total);
    }

    private static void ok(String s) {
        System.out.println("  [OK]      " + s);
    }

    private static void atencao(String s) {
        atencoes++;
        System.out.println("  [ATENÇÃO] " + s);
    }

    private static void erro(String s) {
        erros++;
        System.out.println("  [ERRO]    " + s);
    }
}