package rep03;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Verifica, a partir dos ficheiros reunidos numa pasta, o estado de cada partição.
 *
 * Uso: PartitionCheck <N> <pasta>      (fator de replicação esperado fixo: R = 2)
 *
 * Lê lider-p<k>.txt e replica-<n>-p<k>.txt. Não escreve nada nos ficheiros.
 * Código de saída: 0 se não houver ERRO (as ATENÇÕES, como uma réplica que ainda não convergiu, não
 * contam como erro); 1 se houver pelo menos um ERRO; 2 se os argumentos forem inválidos.
 */
public final class PartitionCheck {

    private static final Pattern LIDER = Pattern.compile("lider-p(\\d+)\\.txt");
    private static final Pattern REPLICA = Pattern.compile("replica-(\\d+)-p(\\d+)\\.txt");

    private static final int R = 2;                 // fator de replicação, fixo pelo enunciado

    private static int erros = 0;
    private static int atencoes = 0;
    private static int numParticoes;

    private PartitionCheck() { }

    // conteúdo de um ficheiro: linhas válidas (texto) e registos já interpretados
    private static final class Conteudo {
        final List<String> linhas = new ArrayList<>();        // linhas não vazias, por ordem
        final List<SensorRecord> registos = new ArrayList<>(); // as que se conseguiram interpretar
        final List<String> invalidas = new ArrayList<>();      // "linha N: texto"
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: PartitionCheck <N> <pasta>");
            System.exit(2);
        }
        int n;
        try {
            n = Integer.parseInt(args[0]);
        } catch (NumberFormatException e) {
            System.out.println("N tem de ser inteiro");
            System.exit(2);
            return;
        }
        numParticoes = n;
        Partitioner particionador;
        try {
            particionador = new Partitioner(n);
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            System.exit(2);
            return;
        }
        Path pasta = Paths.get(args[1]);
        if (!Files.isDirectory(pasta)) {
            System.out.println("Pasta inexistente: " + pasta);
            System.exit(2);
        }

        // --- inventário dos ficheiros da pasta ---
        TreeMap<Integer, Path> lideres = new TreeMap<>();                       // k -> ficheiro
        TreeMap<Integer, TreeMap<Integer, Path>> replicas = new TreeMap<>();    // k -> (n -> ficheiro)
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(pasta)) {
            for (Path p : ds) {
                String nome = p.getFileName().toString();
                Matcher ml = LIDER.matcher(nome);
                Matcher mr = REPLICA.matcher(nome);
                if (ml.matches()) {
                    lideres.put(Integer.parseInt(ml.group(1)), p);
                } else if (mr.matches()) {
                    replicas.computeIfAbsent(Integer.parseInt(mr.group(2)), x -> new TreeMap<>())
                            .put(Integer.parseInt(mr.group(1)), p);
                }
            }
        }

        System.out.println("=== PartitionCheck: N=" + n + ", R esperado=" + R + ", pasta=" + pasta.toAbsolutePath() + " ===");

        // partições com ficheiros mas fora de [0, N): sinal de N diferente entre processos
        Set<Integer> fora = new HashSet<>(lideres.keySet());
        fora.addAll(replicas.keySet());
        for (int k : new java.util.TreeSet<>(fora)) {
            if (k >= n) {
                erro("existem ficheiros da partição p" + k + ", que não existe com N=" + n
                        + " (processos com N diferente?)");
            }
        }

        long somaParticoes = 0;
        for (int k = 0; k < n; k++) {
            System.out.println();
            System.out.println("--- Partição p" + k + " ---");
            Path fl = lideres.get(k);
            Conteudo lider = null;
            if (fl == null) {
                erro("falta lider-p" + k + ".txt");
            } else {
                lider = ler(fl);
                avisarInvalidas(fl, lider);
                Set<String> sensores = new HashSet<>();
                int errados = 0;
                for (SensorRecord s : lider.registos) {
                    sensores.add(s.getSensorId());
                    int j = particionador.of(s.getSensorId());
                    if (j != k) {
                        errados++;
                        if (errados <= 3) {
                            System.out.println("    sensor " + s.getSensorId() + " (seq " + s.getSeq()
                                    + ") pertence a p" + j);
                        }
                    }
                }
                System.out.println("  líder " + fl.getFileName() + ": " + lider.registos.size()
                        + " registos, " + sensores.size() + " sensores distintos");
                if (errados == 0) {
                    ok("todos os registos são de sensores com of(sensor) == " + k);
                } else {
                    erro(errados + " registos de sensores que não pertencem a p" + k);
                }
                verificarSeq(fl.getFileName().toString(), lider);
                somaParticoes += lider.registos.size();
            }

            TreeMap<Integer, Path> rs = replicas.getOrDefault(k, new TreeMap<>());
            if (rs.size() == R) {
                ok("p" + k + " guardada por " + rs.size() + " réplicas " + rs.keySet() + " (R=" + R + ")");
            } else {
                erro("p" + k + " guardada por " + rs.size() + " réplicas " + rs.keySet() + ", esperado R=" + R);
            }
            for (java.util.Map.Entry<Integer, Path> e : rs.entrySet()) {
                Path fr = e.getValue();
                Conteudo rep = ler(fr);
                avisarInvalidas(fr, rep);
                verificarSeq(fr.getFileName().toString(), rep);
                comparar(fr.getFileName().toString(), rep, lider);
            }
        }

        // --- total ---
        System.out.println();
        System.out.println("--- Total ---");
        long totalGlobal = 0;
        for (Path p : lideres.values()) {
            totalGlobal += ler(p).registos.size();    // todos os lider-p*.txt da pasta, mesmo fora de [0, N)
        }
        System.out.println("  registos em todos os lider-p*.txt da pasta: " + totalGlobal);
        System.out.println("  soma dos registos das partições 0.." + (n - 1) + ":      " + somaParticoes);
        if (totalGlobal == somaParticoes) {
            ok("o total do sistema é igual à soma dos registos das partições");
        } else {
            erro("o total (" + totalGlobal + ") difere da soma das partições (" + somaParticoes + ")");
        }

        System.out.println();
        System.out.println("RESULTADO: " + erros + " erro(s), " + atencoes + " atenção(ões)"
                + (erros == 0 ? (atencoes == 0 ? " -> TUDO CONVERGIDO" : " -> sem erros, mas há réplicas por convergir") : " -> FALHAS DETETADAS"));
        System.exit(erros == 0 ? 0 : 1);
    }

    // ---------- leitura ----------

    private static Conteudo ler(Path p) throws IOException {
        Conteudo c = new Conteudo();
        List<String> todas = Files.readAllLines(p, StandardCharsets.UTF_8);
        for (int i = 0; i < todas.size(); i++) {
            String linha = todas.get(i);
            if (linha.isBlank()) {
                continue;
            }
            c.linhas.add(linha.trim());
            try {
                c.registos.add(SensorRecord.fromLine(linha, numParticoes));
            } catch (IllegalArgumentException e) {      // inclui NumberFormatException
                c.invalidas.add("linha " + (i + 1) + ": " + linha);
            }
        }
        return c;
    }

    private static void avisarInvalidas(Path p, Conteudo c) {
        if (!c.invalidas.isEmpty()) {
            erro(p.getFileName() + " tem " + c.invalidas.size() + " linha(s) mal formada(s); a primeira: "
                    + c.invalidas.get(0));
        }
    }

    // ---------- verificações ----------

    // a sequência de seq começa em 1 e não tem saltos nem repetições
    private static void verificarSeq(String nome, Conteudo c) {
        if (c.registos.isEmpty()) {
            System.out.println("  " + nome + ": vazio");
            return;
        }
        long esperado = 1;
        for (int i = 0; i < c.registos.size(); i++) {
            long seq = c.registos.get(i).getSeq();
            if (seq != esperado) {
                String tipo = seq < esperado ? "repetição/regressão" : "salto";
                erro(nome + ": " + tipo + " no registo n.º " + (i + 1) + " (seq " + seq + ", esperava " + esperado + ")");
                return;
            }
            esperado++;
        }
        ok(nome + ": seq 1.." + (esperado - 1) + " sem saltos nem repetições");
    }

    // igual ao líder, prefixo do líder (ainda falta convergir) ou divergente
    private static void comparar(String nome, Conteudo rep, Conteudo lider) {
        if (lider == null) {
            System.out.println("  " + nome + ": " + rep.linhas.size() + " registos (sem líder para comparar)");
            return;
        }
        int nr = rep.linhas.size();
        int nl = lider.linhas.size();
        int comum = Math.min(nr, nl);
        for (int i = 0; i < comum; i++) {
            if (!rep.linhas.get(i).equals(lider.linhas.get(i))) {
                erro(nome + ": DIVERGE do líder na linha " + (i + 1) + "\n        réplica: " + rep.linhas.get(i)
                        + "\n        líder:   " + lider.linhas.get(i));
                return;
            }
        }
        if (nr == nl) {
            ok(nome + ": igual ao líder (" + nr + " registos)");
        } else if (nr < nl) {
            atencao(nome + ": prefixo do líder, faltam " + (nl - nr) + " registos (" + nr + " de " + nl + ")");
        } else {
            erro(nome + ": tem MAIS registos que o líder (" + nr + " > " + nl
                    + "); o líder perdeu ou reiniciou o ficheiro?");
        }
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