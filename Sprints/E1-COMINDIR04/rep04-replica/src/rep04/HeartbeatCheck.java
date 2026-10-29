package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.LinkedHashMap;

// HeartbeatCheck <N> <pasta> [falhas.txt]
// Lê hb-p*.txt, lider-p*.txt e monitor-*.txt de uma pasta e escreve o relatório do ponto 7 da COMINDIR04.
public final class HeartbeatCheck {

    // ---------- uma linha de hb-p<k>.txt: inc;hb;instante;ultimoSeq;n;bytes ----------
    static final class Hb {
        final int k;
        final long inc, hb, instante, ultimoSeq;
        final int n, bytes;

        Hb(int k, String linha) {
            String[] f = linha.trim().split(";");
            if (f.length != 6) {
                throw new IllegalArgumentException("linha de hb mal formada: " + linha);
            }
            this.k = k;
            this.inc = Long.parseLong(f[0]);
            this.hb = Long.parseLong(f[1]);
            this.instante = Long.parseLong(f[2]);
            this.ultimoSeq = Long.parseLong(f[3]);
            this.n = Integer.parseInt(f[4]);
            this.bytes = Integer.parseInt(f[5]);
        }
    }

    // ---------- uma linha de monitor-<id>.txt: instante;EVENTO;p<k>;campo=valor;... ----------
    static final class Evento {
        final String participante;          // "replica1", "processador", "produtor"
        final long instante;
        final String tipo;                  // RECEBIDO, SUSPEITA, RETOMA, REINICIO, HB_PERDIDO, RECUPERACAO
        final int k;
        final Map<String, String> campos = new HashMap<>();   // ex.: silencio_ms -> 15003

        Evento(String participante, String linha) {
            String[] f = linha.trim().split(";");
            if (f.length < 3 || !f[2].startsWith("p")) {
                throw new IllegalArgumentException("evento mal formado: " + linha);
            }
            this.participante = participante;
            this.instante = Long.parseLong(f[0]);
            this.tipo = f[1];
            this.k = Integer.parseInt(f[2].substring(1));
            for (int i = 3; i < f.length; i++) {
                int eq = f[i].indexOf('=');
                if (eq > 0) {
                    campos.put(f[i].substring(0, eq), f[i].substring(eq + 1));
                }
            }
        }
    }

    // ---------- leitura dos ficheiros ----------

    // todos os heartbeats publicados, por partição (lista pela ordem do ficheiro)
    static Map<Integer, List<Hb>> lerHeartbeats(Path pasta, int n) throws IOException {
        Map<Integer, List<Hb>> porParticao = new TreeMap<>();
        for (int k = 0; k < n; k++) {
            List<Hb> lista = new ArrayList<>();
            Path f = pasta.resolve("hb-p" + k + ".txt");
            if (Files.exists(f)) {
                for (String l : Files.readAllLines(f, StandardCharsets.UTF_8)) {
                    if (!l.isBlank()) {
                        lista.add(new Hb(k, l));
                    }
                }
            } else {
                System.out.println("AVISO: não existe " + f.getFileName());
            }
            porParticao.put(k, lista);
        }
        return porParticao;
    }

    // registos dos líderes: partição -> (seq -> registo); serve para a latência e para os bytes do REG
    static Map<Integer, Map<Long, SensorRecord>> lerLideres(Path pasta, int n) throws IOException {
        Map<Integer, Map<Long, SensorRecord>> porParticao = new TreeMap<>();
        for (int k = 0; k < n; k++) {
            Map<Long, SensorRecord> regs = new TreeMap<>();
            Path f = pasta.resolve("lider-p" + k + ".txt");
            if (Files.exists(f)) {
                for (String l : Files.readAllLines(f, StandardCharsets.UTF_8)) {
                    if (!l.isBlank()) {
                        SensorRecord r = SensorRecord.fromLine(l, n);
                        regs.put(r.getSeq(), r);
                    }
                }
            }
            porParticao.put(k, regs);
        }
        return porParticao;
    }

    // eventos de todos os ficheiros monitor-*.txt (o participante sai do nome do ficheiro)
    static List<Evento> lerMonitores(Path pasta) throws IOException {
        List<Evento> eventos = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(pasta, "monitor-*.txt")) {
            for (Path p : ds) {
                String nome = p.getFileName().toString();
                String participante = nome.substring("monitor-".length(), nome.length() - ".txt".length());
                for (String l : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                    if (l.isBlank()) {
                        continue;
                    }
                    try {
                        eventos.add(new Evento(participante, l));
                    } catch (IllegalArgumentException e) {   // inclui NumberFormatException
                        System.out.println("AVISO: linha ignorada em " + nome + ": " + l);
                    }
                }
            }
        }
        eventos.sort((a, b) -> Long.compare(a.instante, b.instante));   // ordem temporal global
        return eventos;
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: HeartbeatCheck <N> <pasta> [falhas.txt]");
            System.exit(2);
        }
        int n = Integer.parseInt(args[0]);
        Path pasta = Paths.get(args[1]);
        if (!Files.isDirectory(pasta)) {
            System.out.println("Pasta inexistente: " + pasta);
            System.exit(2);
        }

        Map<Integer, List<Hb>> hbs = lerHeartbeats(pasta, n);
        Map<Integer, Map<Long, SensorRecord>> lideres = lerLideres(pasta, n);
        List<Evento> eventos = lerMonitores(pasta);

        // verificação do bloco 1: confirmar que a leitura bate certo com os ficheiros
        System.out.println("=== HeartbeatCheck: leitura ===");
        for (int k = 0; k < n; k++) {
            System.out.println("p" + k + ": " + hbs.get(k).size() + " heartbeats, "
                    + lideres.get(k).size() + " registos no líder");
        }
        Map<String, Integer> porTipo = new TreeMap<>();
        for (Evento e : eventos) {
            porTipo.merge(e.participante + " " + e.tipo, 1, Integer::sum);
        }
        porTipo.forEach((chave, conta) -> System.out.println(chave + ": " + conta));
        relatorioLotes(hbs);
        List<Suspeita> suspeitas = relatorioSuspeitas(eventos, hbs);

        // falhas.txt: aceita o caminho tal como foi dado ou relativo à pasta
        Path falhas = null;
        if (args.length > 2) {
            falhas = Paths.get(args[2]);
            if (!Files.exists(falhas)) falhas = pasta.resolve(args[2]);
            if (!Files.exists(falhas)) {
                System.out.println("AVISO: não encontrei " + args[2]);
                falhas = null;
            }
        }
        relatorioDetecao(falhas, suspeitas, eventos);
        relatorioLatencia(eventos, lideres);
        relatorioCustos(eventos, hbs, lideres);
    }
    static void relatorioLotes(Map<Integer, List<Hb>> hbs) {
        System.out.println("\n=== 1. Lotes por partição e incarnação ===");
        for (int k : hbs.keySet()) {
            // agrupa as linhas por incarnação, mantendo a ordem do ficheiro
            Map<Long, List<Hb>> porInc = new LinkedHashMap<>();
            for (Hb h : hbs.get(k)) {
                porInc.computeIfAbsent(h.inc, x -> new ArrayList<>()).add(h);
            }

            for (Map.Entry<Long, List<Hb>> e : porInc.entrySet()) {
                List<Hb> lotes = e.getValue();
                int vazios = 0, somaN = 0, maxN = 0;
                long somaIntervalo = 0, maxIntervalo = 0;
                int numIntervalos = 0;

                for (int i = 0; i < lotes.size(); i++) {
                    Hb h = lotes.get(i);
                    if (h.n == 0) vazios++;
                    somaN += h.n;
                    maxN = Math.max(maxN, h.n);
                    // Se existir lote anterior
                    if(i > 0){
                        long intervalo = h.instante - lotes.get(i-1).instante;
                        somaIntervalo += intervalo;
                        maxIntervalo = Math.max(maxIntervalo, intervalo);
                        numIntervalos++;
                    }

                }

                double propVazios = lotes.isEmpty() ? 0 : 100.0 * vazios / lotes.size();
                double mediaN = lotes.isEmpty() ? 0 : (double) somaN / lotes.size();
                double mediaInterv = numIntervalos == 0 ? 0 : (double) somaIntervalo / numIntervalos;

                System.out.printf("p%d inc=%d: %d lotes, %.1f%% vazios, registos/lote média %.2f máx %d, "
                                + "intervalo média %.0f ms máx %d ms%n",
                        k, e.getKey(), lotes.size(), propVazios, mediaN, maxN, mediaInterv, maxIntervalo);
            }
        }
    }
    // ---------- BLOCO 3: suspeitas classificadas como falha real ou falsa suspeita ----------
    static final class Suspeita {
        final Evento ev;        // a linha SUSPEITA do monitor
        final Hb ultimoHb;      // último lote publicado pelo líder antes da suspeita
        final boolean real;     // true = falha real; false = falsa suspeita

        Suspeita(Evento ev, Hb ultimoHb, boolean real) {
            this.ev = ev;
            this.ultimoHb = ultimoHb;
            this.real = real;
        }
    }

    static List<Suspeita> relatorioSuspeitas(List<Evento> eventos, Map<Integer, List<Hb>> hbs) {
        System.out.println("\n=== 2. Suspeitas por participante e partição ===");
        List<Suspeita> resultado = new ArrayList<>();
        int reais = 0, falsas = 0, semLider = 0;

        for (Evento ev : eventos) {
            if (!ev.tipo.equals("SUSPEITA")) {
                continue;
            }
            List<Hb> lista = hbs.getOrDefault(ev.k, new ArrayList<>());

            // 1) último lote que o líder publicou ANTES da suspeita (o ficheiro está por ordem temporal)
            Hb ultimo = null;
            for (Hb h : lista) {
                if (h.instante <= ev.instante) {
                    ultimo = h;
                }
            }
            if (ultimo == null) {
                System.out.println(ev.participante + " p" + ev.k + " t=" + ev.instante
                        + ": suspeita sem nenhum heartbeat anterior (o líder ainda não tinha arrancado?)");
                semLider++;
                continue;
            }

            // 2) a mesma incarnação voltou a publicar depois da suspeita? se sim, o líder estava vivo
            boolean continuou = false;
            for (Hb h : lista) {
                if (h.inc == ultimo.inc && h.instante > ev.instante) {
                    continuou = true;
                    break;
                }
            }
            boolean real = !continuou;
            if (real) reais++; else falsas++;
            resultado.add(new Suspeita(ev, ultimo, real));

            // 3) confirmação pelo lado do participante: o RETOMA seguinte traz o mesmo inc ou um novo?
            String retoma = "sem RETOMA";
            for (Evento r : eventos) {
                if (r.instante > ev.instante && r.tipo.equals("RETOMA")
                        && r.participante.equals(ev.participante) && r.k == ev.k) {
                    boolean mesmo = String.valueOf(ultimo.inc).equals(r.campos.get("inc"));
                    retoma = "RETOMA inc=" + r.campos.get("inc") + (mesmo ? " (mesma incarnação)" : " (nova incarnação)");
                    break;
                }
            }

            long silencio = ev.instante - ultimo.instante;
            System.out.println(ev.participante + " p" + ev.k + " t=" + ev.instante
                    + ": silêncio desde o último heartbeat publicado = " + silencio + " ms -> "
                    + (real ? "FALHA REAL" : "FALSA SUSPEITA") + " | " + retoma);
        }
        System.out.println("Total: " + reais + " falhas reais, " + falsas + " falsas suspeitas, "
                + semLider + " sem heartbeat anterior");
        return resultado;
    }

    // ---------- BLOCO 4: tempo de deteção de cada falha real (só com falhas.txt) ----------
    // falhas.txt: uma linha "p<k>;instante" por cada falha provocada (kill -9)
    static void relatorioDetecao(Path falhasFile, List<Suspeita> suspeitas, List<Evento> eventos)
            throws IOException {
        System.out.println("\n=== 3. Tempo de deteção das falhas reais ===");
        if (falhasFile == null) {
            System.out.println("(sem falhas.txt: secção omitida)");
            return;
        }

        // falhas provocadas, por partição e por ordem temporal
        Map<Integer, List<Long>> falhas = new TreeMap<>();
        for (String l : Files.readAllLines(falhasFile, StandardCharsets.UTF_8)) {
            if (l.isBlank()) continue;
            String[] f = l.trim().split(";");
            falhas.computeIfAbsent(Integer.parseInt(f[0].substring(1)), x -> new ArrayList<>())
                    .add(Long.parseLong(f[1]));
        }

        // participantes que acompanham cada partição (todos os que têm eventos dela)
        Map<Integer, java.util.Set<String>> acompanham = new TreeMap<>();
        for (Evento e : eventos) {
            acompanham.computeIfAbsent(e.k, x -> new java.util.TreeSet<>()).add(e.participante);
        }

        long previstoMin = Wire.PERIODO_HB_MS * (Wire.FATOR_TIMEOUT - 1);
        long previstoMax = Wire.PERIODO_HB_MS * Wire.FATOR_TIMEOUT + Wire.VERIFICACAO_MS;
        System.out.println("Intervalo previsto: ]" + previstoMin + ", " + previstoMax + "] ms"
                + " (Δ=" + Wire.PERIODO_HB_MS + ", fator=" + Wire.FATOR_TIMEOUT + ")");

        long soma = 0, min = Long.MAX_VALUE, max = 0;
        int conta = 0, fora = 0;
        for (Map.Entry<Integer, List<Long>> e : falhas.entrySet()) {
            int k = e.getKey();
            List<Long> lista = e.getValue();
            lista.sort(null);
            for (int i = 0; i < lista.size(); i++) {
                long tFalha = lista.get(i);
                long limite = i + 1 < lista.size() ? lista.get(i + 1) : Long.MAX_VALUE;   // até à falha seguinte
                System.out.println("falha p" + k + " em t=" + tFalha + ":");
                for (String part : acompanham.getOrDefault(k, new java.util.TreeSet<>())) {
                    // primeira suspeita REAL deste participante, nesta partição, depois da falha
                    Suspeita det = null;
                    for (Suspeita s : suspeitas) {
                        if (s.real && s.ev.k == k && s.ev.participante.equals(part)
                                && s.ev.instante > tFalha && s.ev.instante < limite) {
                            det = s;
                            break;                    // a lista está por ordem temporal
                        }
                    }
                    if (det == null) {
                        System.out.println("   " + part + ": não detetou");
                        continue;
                    }
                    long t = det.ev.instante - tFalha;
                    boolean dentro = t > previstoMin && t <= previstoMax;
                    if (!dentro) fora++;
                    System.out.println("   " + part + ": " + t + " ms" + (dentro ? "" : "  <-- FORA do intervalo previsto"));
                    soma += t;
                    min = Math.min(min, t);
                    max = Math.max(max, t);
                    conta++;
                }
            }
        }
        if (conta > 0) {
            System.out.printf("Tempo de deteção: mínimo %d ms, médio %.0f ms, máximo %d ms (%d deteções, %d fora do previsto)%n",
                    min, (double) soma / conta, max, conta, fora);
        }
    }

    // ---------- BLOCO 5: latência de lote (RECEBIDO na réplica − instante do registo no líder) ----------
    static void relatorioLatencia(List<Evento> eventos, Map<Integer, Map<Long, SensorRecord>> lideres) {
        System.out.println("\n=== 4. Latência de lote por partição ===");
        Map<Integer, long[]> porParticao = new TreeMap<>();   // k -> {soma, máximo, contagem}
        for (Evento e : eventos) {
            if (!e.tipo.equals("RECEBIDO")) continue;
            String intervalo = e.campos.get("intervalo");
            if (intervalo == null || intervalo.equals("-")) continue;          // lote vazio
            String[] ab = intervalo.split("\\.\\.");
            long a = Long.parseLong(ab[0]), b = Long.parseLong(ab[1]);
            for (long seq = a; seq <= b; seq++) {
                SensorRecord r = lideres.get(e.k).get(seq);
                if (r == null) continue;                                     // não está no ficheiro do líder
                long lat = e.instante - r.getTimestampMillis();
                long[] acc = porParticao.computeIfAbsent(e.k, x -> new long[3]);
                acc[0] += lat;
                acc[1] = Math.max(acc[1], lat);
                acc[2]++;
            }
        }
        for (Map.Entry<Integer, long[]> e : porParticao.entrySet()) {
            long[] acc = e.getValue();
            System.out.printf("p%d: média %.0f ms, máximo %d ms (%d receções de registos)%n",
                    e.getKey(), (double) acc[0] / acc[2], acc[1], acc[2]);
        }
        System.out.println("(valor esperado da média: cerca de Δ/2 = " + Wire.PERIODO_HB_MS / 2 + " ms)");
    }

    // ---------- BLOCO 6: contagens de eventos e custo em mensagens e bytes ----------
    static void relatorioCustos(List<Evento> eventos, Map<Integer, List<Hb>> hbs,
                                Map<Integer, Map<Long, SensorRecord>> lideres) {
        System.out.println("\n=== 5. HB_PERDIDO e RECUPERACAO ===");
        Map<String, Integer> contagem = new TreeMap<>();
        for (Evento e : eventos) {
            if (e.tipo.equals("HB_PERDIDO") || e.tipo.equals("RECUPERACAO")) {
                contagem.merge(e.tipo + " " + e.participante + " p" + e.k, 1, Integer::sum);
            }
        }
        if (contagem.isEmpty()) System.out.println("nenhum");
        contagem.forEach((c, v) -> System.out.println(c + ": " + v));

        System.out.println("\n=== 6. Mensagens e bytes: COMINDIR03 (REG) vs COMINDIR04 (LOTE) ===");
        long lotes = 0, lotesComDados = 0, bytesLotesComDados = 0, registosEmLote = 0, bytesTodosLotes = 0;
        for (List<Hb> lista : hbs.values()) {
            for (Hb h : lista) {
                lotes++;
                bytesTodosLotes += h.bytes;
                if (h.n > 0) {
                    lotesComDados++;
                    bytesLotesComDados += h.bytes;
                    registosEmLote += h.n;
                }
            }
        }
        long registos = 0, bytesReg = 0;
        for (Map.Entry<Integer, Map<Long, SensorRecord>> e : lideres.entrySet()) {
            for (SensorRecord r : e.getValue().values()) {
                // a mensagem que a COMINDIR03 publicaria para este registo
                String reg = "REG;127.0.0.1;" + (Wire.PORTO_BASE + e.getKey()) + ";" + r.toLine();
                bytesReg += reg.getBytes(StandardCharsets.UTF_8).length;
                registos++;
            }
        }
        System.out.println("Mensagens no tópico de dados: COMINDIR03 = " + registos + " REG, COMINDIR04 = "
                + lotes + " LOTE (" + lotesComDados + " com dados, " + (lotes - lotesComDados) + " vazios)");
        if (registos > 0) {
            System.out.printf("Bytes por registo: REG individual %.1f B%n", (double) bytesReg / registos);
        }
        if (registosEmLote > 0) {
            System.out.printf("Bytes por registo: em LOTE %.1f B (cabeçalho repartido pelos registos do lote)%n",
                    (double) bytesLotesComDados / registosEmLote);
        }
        System.out.println("Bytes totais no tópico de dados: COMINDIR03 = " + bytesReg
                + " B, COMINDIR04 = " + bytesTodosLotes + " B (inclui os lotes vazios)");
    }
}