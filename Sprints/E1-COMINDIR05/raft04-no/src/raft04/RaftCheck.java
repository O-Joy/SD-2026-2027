package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * RaftCheck (COMINDIR05, ponto 8): análise a posteriori dos ficheiros de um ensaio.
 *
 *   RaftCheck <N> <pasta> [falhas.txt]
 *
 * Lê, da pasta:
 *   no-p<k>-n<i>.txt         seq;termo;sensor;temperatura;instante;L;V
 *   estado-p<k>-n<i>.txt     termo;votouEm
 *   hb-p<k>-n<i>.txt         termo;hb;instante;ultimoSeq;ultimoTermo;n;bytes
 *   eventos-p<k>-n<i>.txt    instante;EVENTO;p<k>;detalhe
 *   truncados-p<k>-n<i>.txt  instante;<linha de registo descartada>   (acrescentado pelo C ao PartitionNode)
 *   monitor-<id>.txt         instante;EVENTO;p<k>;detalhe (produtor e processador)
 *   produtor.txt             instante;p<k>;sensor;temperatura
 * e, opcionalmente, falhas.txt com linhas p<k>;n<i>;instante (kill -9 ou início de um isolamento).
 *
 * Correspondência entre leituras e registos: as leituras não têm identificador único. Uma linha de
 * produtor.txt corresponde a um registo da mesma partição com o mesmo sensor, a mesma temperatura e
 * um instante entre [envio - TOL_ANTES_MS, envio + JANELA_MS]; cada registo serve uma só leitura.
 */
public final class RaftCheck {

    static final long JANELA_MS = 5000;        // atraso máximo aceite entre o envio e o registo no líder
    static final long TOL_ANTES_MS = 50;       // folga para arredondamentos (mesmo relógio, mesma máquina)

    private static final Pattern FICH = Pattern.compile("(no|estado|hb|eventos|truncados)-p(\\d+)-n(\\d+)\\.txt");
    private static final Pattern MONITOR = Pattern.compile("monitor-(.+)\\.txt");

    // ------------------------------------------------------------------ estruturas

    static final class Evento {
        final long t; final String tipo; final int k; final int no; final String linha;
        final Map<String, String> campos = new HashMap<>();
        Evento(long t, String tipo, int k, int no, String linha) {
            this.t = t; this.tipo = tipo; this.k = k; this.no = no; this.linha = linha;
        }
        long termo() { String v = campos.get("termo"); return v == null ? -1 : Long.parseLong(v); }
    }

    static final class Hb {
        final int no; final long termo, hb, t, ultimoSeq, ultimoTermo; final int n; final int bytes;
        Hb(int no, String[] f) {
            this.no = no;
            termo = Long.parseLong(f[0]); hb = Long.parseLong(f[1]); t = Long.parseLong(f[2]);
            ultimoSeq = Long.parseLong(f[3]); ultimoTermo = Long.parseLong(f[4]);
            n = Integer.parseInt(f[5]); bytes = Integer.parseInt(f[6]);
        }
    }

    static final class Leitura {
        final long t; final int k; final String sensor; final double temp;
        Leitura(long t, int k, String sensor, double temp) { this.t = t; this.k = k; this.sensor = sensor; this.temp = temp; }
        public String toString() { return sensor + " " + temp + " (enviada em " + t + ")"; }
    }

    static final class Particao {
        final TreeMap<Integer, List<SensorRecord>> registos = new TreeMap<>();   // nó -> registos finais
        final TreeMap<Integer, long[]> estado = new TreeMap<>();                 // nó -> {termo, votouEm}
        final List<Hb> hbs = new ArrayList<>();
        final List<Evento> eventos = new ArrayList<>();
        final TreeMap<Integer, List<SensorRecord>> truncados = new TreeMap<>();  // nó -> descartados
        final TreeSet<Integer> nos = new TreeSet<>();
    }

    // ------------------------------------------------------------------ main

    private static int n;
    private static final TreeMap<Integer, Particao> parts = new TreeMap<>();
    private static final List<Leitura> leituras = new ArrayList<>();
    private static final TreeMap<String, List<Evento>> monitores = new TreeMap<>();

    // resumo para as tabelas do ponto 11
    private static final List<Long> tempos1oLider = new ArrayList<>();
    private static final List<Long> termos1oLider = new ArrayList<>();
    private static int termosSemLiderTotal = 0, votosDivididosTotal = 0, termosDoisLideres = 0, seqDivergentesTotal = 0;
    private static final List<Long> indisponibilidades = new ArrayList<>();
    private static int perdidasEleicao = 0, perdidasTruncagem = 0, truncadosTotal = 0;

    public static void main(String[] args) throws IOException {
        if (args.length < 2) {
            System.out.println("Uso: RaftCheck <N> <pasta> [falhas.txt]");
            System.exit(2);
        }
        n = Integer.parseInt(args[0]);
        Path pasta = Paths.get(args[1]);
        if (!Files.isDirectory(pasta)) {
            System.out.println("Pasta inexistente: " + pasta);
            System.exit(2);
        }
        ler(pasta);

        System.out.println("=== RaftCheck: N=" + n + ", pasta=" + pasta.toAbsolutePath() + " ===");
        System.out.println("Parâmetros: Δ=" + Wire.PERIODO_HB_MS + " ms, prazo de eleição [" + Wire.ELEICAO_MIN_MS
                + ", " + Wire.ELEICAO_MAX_MS + "] ms, verificação " + Wire.VERIFICACAO_MS + " ms");

        relatorioTermos();
        relatorioSeguranca();
        relatorioPrimeiroLider();
        if (args.length > 2) relatorioIndisponibilidade(Paths.get(args[2]));
        else System.out.println("\n=== 4. Indisponibilidade ===\n(sem falhas.txt: secção omitida)");
        relatorioPerdidas();
        relatorioTruncados();
        relatorioCoincidencia();
        resumo();
    }

    // ------------------------------------------------------------------ leitura dos ficheiros

    private static void ler(Path pasta) throws IOException {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(pasta)) {
            for (Path p : ds) {
                String nome = p.getFileName().toString();
                Matcher m = FICH.matcher(nome);
                if (m.matches()) {
                    String tipo = m.group(1);
                    int k = Integer.parseInt(m.group(2));
                    int no = Integer.parseInt(m.group(3));
                    Particao pt = parts.computeIfAbsent(k, x -> new Particao());
                    pt.nos.add(no);
                    List<String> linhas = Files.readAllLines(p, StandardCharsets.UTF_8);
                    switch (tipo) {
                        case "no":
                            List<SensorRecord> rs = new ArrayList<>();
                            for (String l : linhas) if (!l.isBlank()) rs.add(SensorRecord.fromLine(l, n));
                            pt.registos.put(no, rs);
                            break;
                        case "estado":
                            for (String l : linhas) if (!l.isBlank()) {
                                String[] f = l.trim().split(";");
                                pt.estado.put(no, new long[] { Long.parseLong(f[0]), Long.parseLong(f[1]) });
                            }
                            break;
                        case "hb":
                            for (String l : linhas) if (!l.isBlank()) pt.hbs.add(new Hb(no, l.trim().split(";")));
                            break;
                        case "eventos":
                            for (String l : linhas) {
                                Evento e = evento(l, k, no);
                                if (e != null) pt.eventos.add(e);
                            }
                            break;
                        case "truncados":
                            List<SensorRecord> ts = new ArrayList<>();
                            for (String l : linhas) if (!l.isBlank()) {
                                ts.add(SensorRecord.fromLine(l.substring(l.indexOf(';') + 1), n));
                            }
                            pt.truncados.put(no, ts);
                            break;
                        default:
                    }
                    continue;
                }
                Matcher mm = MONITOR.matcher(nome);
                if (mm.matches()) {
                    List<Evento> evs = new ArrayList<>();
                    for (String l : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                        Evento e = evento(l, -1, 0);
                        if (e != null) evs.add(e);
                    }
                    monitores.put(mm.group(1), evs);
                } else if (nome.equals("produtor.txt")) {
                    for (String l : Files.readAllLines(p, StandardCharsets.UTF_8)) {
                        if (l.isBlank()) continue;
                        String[] f = l.trim().split(";");
                        leituras.add(new Leitura(Long.parseLong(f[0]), Integer.parseInt(f[1].substring(1)),
                                f[2], Double.parseDouble(f[3])));
                    }
                }
            }
        }
        for (Particao pt : parts.values()) pt.eventos.sort((a, b) -> Long.compare(a.t, b.t));
        for (List<Evento> l : monitores.values()) l.sort((a, b) -> Long.compare(a.t, b.t));
        leituras.sort((a, b) -> Long.compare(a.t, b.t));
    }

    // "instante;EVENTO;p<k>;campo=valor;..." (linhas que não começam por um instante são ignoradas)
    private static Evento evento(String linha, int k, int no) {
        String[] f = linha.trim().split(";");
        if (f.length < 2) return null;
        long t;
        try { t = Long.parseLong(f[0]); } catch (NumberFormatException e) { return null; }
        int kk = k;
        if (kk < 0 && f.length > 2 && f[2].startsWith("p")) {
            try { kk = Integer.parseInt(f[2].substring(1)); } catch (NumberFormatException e) { kk = -1; }
        }
        Evento e = new Evento(t, f[1], kk, no, linha);
        for (int i = 2; i < f.length; i++) {
            int eq = f[i].indexOf('=');
            if (eq > 0) e.campos.put(f[i].substring(0, eq), f[i].substring(eq + 1));
            else if (f[i].contains("->")) {                          // TERMO;p1;1->2
                String[] ab = f[i].split("->");
                e.campos.put("de", ab[0]);
                e.campos.put("termo", ab[1]);
            } else if (f[i].equals("concedido") || f[i].equals("recusado")) {
                e.campos.put("decisao", f[i]);
            }
        }
        return e;
    }

    // ------------------------------------------------------------------ 1. termos e eleições

    static final class Termo {
        final TreeSet<Integer> candidatos = new TreeSet<>();
        final Map<Integer, TreeSet<Integer>> votos = new TreeMap<>();   // candidato -> eleitores (inclui ele próprio)
        final TreeSet<Integer> lideres = new TreeSet<>();
        long primeiroCandidato = Long.MAX_VALUE, lider = -1;
    }

    private static TreeMap<Long, Termo> termos(Particao pt) {
        TreeMap<Long, Termo> ts = new TreeMap<>();
        for (Evento e : pt.eventos) {
            long t = e.termo();
            switch (e.tipo) {
                case "CANDIDATO": {
                    Termo x = ts.computeIfAbsent(t, z -> new Termo());
                    x.candidatos.add(e.no);
                    x.votos.computeIfAbsent(e.no, z -> new TreeSet<>()).add(e.no);
                    x.primeiroCandidato = Math.min(x.primeiroCandidato, e.t);
                    break;
                }
                case "VOTO": {
                    if (!"concedido".equals(e.campos.get("decisao"))) break;
                    int c = Integer.parseInt(e.campos.get("candidato").substring(1));
                    ts.computeIfAbsent(t, z -> new Termo()).votos.computeIfAbsent(c, z -> new TreeSet<>()).add(e.no);
                    break;
                }
                case "LIDER": {
                    Termo x = ts.computeIfAbsent(t, z -> new Termo());
                    x.lideres.add(e.no);
                    if (x.lider < 0) x.lider = e.t;
                    break;
                }
                default:
            }
        }
        return ts;
    }

    private static void relatorioTermos() {
        System.out.println("\n=== 1. Termos e eleições por partição ===");
        for (Map.Entry<Integer, Particao> pe : parts.entrySet()) {
            int k = pe.getKey();
            TreeMap<Long, Termo> ts = termos(pe.getValue());
            System.out.println("p" + k + ": " + ts.size() + " termo(s) com eleição");
            for (Map.Entry<Long, Termo> te : ts.entrySet()) {
                Termo x = te.getValue();
                StringBuilder sb = new StringBuilder("  termo " + te.getKey() + ": candidatos " + nos(x.candidatos) + ", votos recebidos (incl. o próprio) ");
                for (Map.Entry<Integer, TreeSet<Integer>> v : x.votos.entrySet()) {
                    sb.append("n").append(v.getKey()).append("=").append(v.getValue().size()).append(" ");
                }
                if (!x.lideres.isEmpty()) {
                    sb.append("-> LÍDER ").append(nos(x.lideres));
                    if (x.primeiroCandidato != Long.MAX_VALUE) {
                        sb.append(" (eleição ").append(x.lider - x.primeiroCandidato).append(" ms)");
                    }
                } else if (x.candidatos.size() >= 2) {
                    sb.append("-> sem líder: VOTO DIVIDIDO");
                    votosDivididosTotal++;
                    termosSemLiderTotal++;
                } else {
                    sb.append("-> sem líder (sem maioria: nós em falta, isolados ou votos recusados)");
                    termosSemLiderTotal++;
                }
                System.out.println(sb);
            }
        }
    }

    // ------------------------------------------------------------------ 2. segurança

    private static void relatorioSeguranca() {
        System.out.println("\n=== 2. Segurança da eleição ===");
        int violacoesTermo = 0, errosDoisLideres = 0;
        for (Map.Entry<Integer, Particao> pe : parts.entrySet()) {
            int k = pe.getKey();
            Particao pt = pe.getValue();
            for (Map.Entry<Long, Termo> te : termos(pt).entrySet()) {
                if (te.getValue().lideres.size() > 1) {
                    termosDoisLideres++;
                    System.out.println("  [ERRO] p" + k + " termo " + te.getKey() + ": " + te.getValue().lideres.size()
                            + " líderes " + nos(te.getValue().lideres));
                }
            }
            // o termo de cada nó nunca diminui (inclui reinícios: ARRANQUE traz o termo lido do ficheiro)
            for (int no : pt.nos) {
                long atual = -1;
                for (Evento e : pt.eventos) {
                    if (e.no != no) continue;
                    if (e.tipo.equals("ERRO") && e.linha.contains("dois lideres")) errosDoisLideres++;
                    if (e.tipo.equals("RECEBIDO") || e.tipo.equals("IGNORADO")) continue;   // termo do lote, não do nó
                    long t = e.termo();
                    if (t < 0) continue;
                    if (e.tipo.equals("TERMO")) {
                        long de = Long.parseLong(e.campos.get("de"));
                        if (t <= de) { violacoesTermo++; System.out.println("  [ERRO] p" + k + " n" + no + ": TERMO " + de + "->" + t); }
                    }
                    if (t < atual) {
                        violacoesTermo++;
                        System.out.println("  [ERRO] p" + k + " n" + no + ": termo diminuiu de " + atual + " para " + t
                                + " (" + e.tipo + " em " + e.t + ")");
                    }
                    atual = Math.max(atual, t);
                }
                long[] est = pt.estado.get(no);
                if (est != null && est[0] < atual) {
                    violacoesTermo++;
                    System.out.println("  [ERRO] p" + k + " n" + no + ": estado gravado com termo " + est[0]
                            + " < termo observado " + atual);
                }
            }
        }

        System.out.println("  termos com mais de um LIDER: " + termosDoisLideres + (termosDoisLideres == 0 ? "  [OK]" : "  [ERRO]"));
        System.out.println("  termos de nó que diminuíram: " + violacoesTermo + (violacoesTermo == 0 ? "  [OK]" : "  [ERRO]"));
        System.out.println("  eventos ERRO 'dois lideres': " + errosDoisLideres + (errosDoisLideres == 0 ? "  [OK]" : "  [ERRO]"));
    }

    // ------------------------------------------------------------------ 3. primeiro líder

    private static void relatorioPrimeiroLider() {
        System.out.println("\n=== 3. Tempo até ao primeiro líder ===");
        long arranqueSistema = Long.MAX_VALUE;
        for (Particao pt : parts.values()) {
            for (Evento e : pt.eventos) if (e.tipo.equals("ARRANQUE")) { arranqueSistema = Math.min(arranqueSistema, e.t); break; }
        }
        for (Map.Entry<Integer, Particao> pe : parts.entrySet()) {
            Particao pt = pe.getValue();
            long arranque = -1;
            Evento lider = null;
            for (Evento e : pt.eventos) {
                if (arranque < 0 && e.tipo.equals("ARRANQUE")) arranque = e.t;
                if (arranque >= 0 && e.tipo.equals("LIDER")) { lider = e; break; }
            }
            if (arranque < 0 || lider == null) {
                System.out.println("p" + pe.getKey() + ": sem " + (arranque < 0 ? "ARRANQUE" : "LIDER"));
                continue;
            }
            long d = lider.t - arranque;
            tempos1oLider.add(d);
            termos1oLider.add(lider.termo());
            System.out.println("p" + pe.getKey() + ": " + d + " ms desde o 1.º nó da partição ("
                    + (lider.t - arranqueSistema) + " ms desde o 1.º nó do sistema); líder n" + lider.no
                    + " no termo " + lider.termo() + " (" + lider.termo() + " termo(s) até haver líder)");
        }
        System.out.println("(previsto, com arranque simultâneo: entre " + Wire.ELEICAO_MIN_MS + " e "
                + (Wire.ELEICAO_MAX_MS + Wire.VERIFICACAO_MS) + " ms, mais a votação)");
    }

    // ------------------------------------------------------------------ 4. indisponibilidade

    private static void relatorioIndisponibilidade(Path falhasFile) throws IOException {
        System.out.println("\n=== 4. Indisponibilidade após cada falha ===");
        long min = Wire.ELEICAO_MIN_MS - Wire.PERIODO_HB_MS, max = Wire.ELEICAO_MAX_MS + Wire.VERIFICACAO_MS;
        System.out.println("Intervalo previsto: ]" + min + "; " + max + "] ms");
        for (String l : Files.readAllLines(falhasFile, StandardCharsets.UTF_8)) {
            if (l.isBlank()) continue;
            String[] f = l.trim().split(";");
            int k = Integer.parseInt(f[0].substring(1));
            int no = Integer.parseInt(f[1].substring(1));
            long tf = Long.parseLong(f[2]);
            Particao pt = parts.get(k);
            if (pt == null) { System.out.println("p" + k + ": sem ficheiros"); continue; }
            // termo do nó no instante da falha
            long termoFalha = -1;
            boolean eraLider = false;
            for (Evento e : pt.eventos) {
                if (e.no != no || e.t > tf) continue;
                if (e.tipo.equals("RECEBIDO") || e.tipo.equals("IGNORADO")) continue;
                if (e.termo() >= 0) termoFalha = Math.max(termoFalha, e.termo());
                if (e.tipo.equals("LIDER")) eraLider = true;
                if (e.tipo.equals("TERMO") || e.tipo.equals("CANDIDATO")) eraLider = false;
            }
            // primeiro LOTE de um líder de termo superior, depois da falha
            Hb novo = null;
            for (Hb h : pt.hbs) {
                if (h.t > tf && h.termo > termoFalha && (novo == null || h.t < novo.t)) novo = h;
            }
            System.out.print("p" + k + " n" + no + " em t=" + tf + " (termo " + termoFalha + (eraLider ? ", era líder" : ", NÃO era líder") + "): ");
            if (novo == null) {
                System.out.println("nenhum LOTE de termo superior depois da falha");
                continue;
            }
            long d = novo.t - tf;
            indisponibilidades.add(d);
            int enviadas = 0;
            for (Leitura r : leituras) if (r.k == k && r.t >= tf && r.t < novo.t) enviadas++;
            System.out.println(d + " ms até ao 1.º LOTE de n" + novo.no + " (termo " + novo.termo + ")"
                    + (d > min && d <= max ? "" : "  <-- FORA do intervalo previsto")
                    + "; leituras enviadas nesse intervalo: " + enviadas);
        }
    }

    // ------------------------------------------------------------------ 5. leituras perdidas

    // regista os registos finais da partição k (união dos nós, sem repetir (seq, termo))
    private static List<SensorRecord> registosFinais(Particao pt) {
        Map<String, SensorRecord> u = new LinkedHashMap<>();
        for (List<SensorRecord> rs : pt.registos.values()) {
            for (SensorRecord r : rs) u.putIfAbsent(r.getSeq() + ":" + r.getTerm(), r);
        }
        return new ArrayList<>(u.values());
    }

    // emparelha leituras com registos (mesmo sensor e temperatura, instante dentro da janela); devolve as não emparelhadas
    private static List<Leitura> naoEmparelhadas(List<Leitura> ls, List<SensorRecord> rs) {
        List<SensorRecord> livres = new ArrayList<>(rs);
        livres.sort((a, b) -> Long.compare(a.getTimestampMillis(), b.getTimestampMillis()));
        List<Leitura> faltam = new ArrayList<>();
        for (Leitura l : ls) {
            SensorRecord achado = null;
            for (SensorRecord r : livres) {
                long tr = r.getTimestampMillis();
                if (tr < l.t - TOL_ANTES_MS) continue;
                if (tr > l.t + JANELA_MS) break;
                if (r.getSensorId().equals(l.sensor) && r.getTemperature() == l.temp) { achado = r; break; }
            }
            if (achado != null) livres.remove(achado); else faltam.add(l);
        }
        return faltam;
    }

    private static void relatorioPerdidas() {
        System.out.println("\n=== 5. Leituras perdidas (produtor.txt sem registo final em nenhum nó) ===");
        if (leituras.isEmpty()) { System.out.println("(sem produtor.txt)"); return; }
        for (Map.Entry<Integer, Particao> pe : parts.entrySet()) {
            int k = pe.getKey();
            List<Leitura> ls = new ArrayList<>();
            for (Leitura l : leituras) if (l.k == k) ls.add(l);
            List<Leitura> faltam = naoEmparelhadas(ls, registosFinais(pe.getValue()));
            List<SensorRecord> trunc = new ArrayList<>();
            for (List<SensorRecord> t : pe.getValue().truncados.values()) trunc.addAll(t);
            List<Leitura> semTrunc = naoEmparelhadas(faltam, trunc);       // as que nem nos truncados aparecem
            int porTrunc = faltam.size() - semTrunc.size();
            perdidasTruncagem += porTrunc;
            perdidasEleicao += semTrunc.size();
            System.out.println("p" + k + ": enviadas " + ls.size() + ", perdidas " + faltam.size()
                    + " (sem líder/eleição: " + semTrunc.size() + ", truncadas: " + porTrunc + ")");
            int c = 0;
            for (Leitura l : faltam) {
                if (c++ == 10) { System.out.println("    ..."); break; }
                System.out.println("    " + l + (semTrunc.contains(l) ? "" : "  [truncada]"));
            }
        }
        System.out.println("(correspondência por sensor + temperatura + janela de " + JANELA_MS + " ms; ver comentário da classe)");
    }

    // ------------------------------------------------------------------ 6. truncagens

    private static void relatorioTruncados() {
        System.out.println("\n=== 6. Registos truncados ===");
        for (Map.Entry<Integer, Particao> pe : parts.entrySet()) {
            int k = pe.getKey();
            Particao pt = pe.getValue();
            int eventosTrunc = 0;
            for (Evento e : pt.eventos) {
                if (!e.tipo.equals("TRUNCADO")) continue;
                eventosTrunc++;
                int d = Integer.parseInt(e.campos.getOrDefault("descartados", "0"));
                truncadosTotal += d;
                System.out.println("p" + k + " n" + e.no + " t=" + e.t + ": de seq " + e.campos.get("de") + ", "
                        + d + " descartado(s), termos " + e.campos.get("termos"));
            }
            if (eventosTrunc == 0) { System.out.println("p" + k + ": nenhum TRUNCADO"); continue; }

            // líder final: o nó que foi LIDER no maior termo
            int liderFinal = -1; long termoFinal = -1;
            for (Evento e : pt.eventos) if (e.tipo.equals("LIDER") && e.termo() >= termoFinal) { termoFinal = e.termo(); liderFinal = e.no; }
            List<SensorRecord> doLider = pt.registos.getOrDefault(liderFinal, new ArrayList<>());
            for (Map.Entry<Integer, List<SensorRecord>> te : pt.truncados.entrySet()) {
                if (te.getValue().isEmpty()) continue;
                List<SensorRecord> livres = new ArrayList<>(doLider);
                int comOutroSeq = 0;
                for (SensorRecord r : te.getValue()) {
                    SensorRecord achado = null;
                    for (SensorRecord x : livres) {
                        if (x.getSensorId().equals(r.getSensorId()) && x.getTemperature() == r.getTemperature()
                                && Math.abs(x.getTimestampMillis() - r.getTimestampMillis()) <= JANELA_MS
                                && !(x.getSeq() == r.getSeq() && x.getTerm() == r.getTerm())) { achado = x; break; }
                    }
                    if (achado != null) { livres.remove(achado); comOutroSeq++; }
                }
                System.out.println("  n" + te.getKey() + ": " + te.getValue().size() + " registo(s) descartado(s); "
                        + comOutroSeq + " existe(m) com outro seq no registo final do líder n" + liderFinal
                        + " (termo " + termoFinal + "), " + (te.getValue().size() - comOutroSeq) + " perdido(s)");
            }
        }
    }

    // ------------------------------------------------------------------ 7. coincidência final

    private static void relatorioCoincidencia() {
        System.out.println("\n=== 7. Coincidência final dos registos dos nós ===");
        for (Map.Entry<Integer, Particao> pe : parts.entrySet()) {
            Particao pt = pe.getValue();
            if (pt.registos.isEmpty()) { System.out.println("p" + pe.getKey() + ": sem ficheiros no-*"); continue; }
            long minUltimo = Long.MAX_VALUE;
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<Integer, List<SensorRecord>> e : pt.registos.entrySet()) {
                List<SensorRecord> rs = e.getValue();
                long u = rs.isEmpty() ? 0 : rs.get(rs.size() - 1).getSeq();
                minUltimo = Math.min(minUltimo, u);
                sb.append(" n").append(e.getKey()).append("=").append(u);
            }
            List<Long> difs = new ArrayList<>();
            for (long s = 1; s <= minUltimo; s++) {
                String ref = null; boolean igual = true;
                for (List<SensorRecord> rs : pt.registos.values()) {
                    String linha = s - 1 < rs.size() && rs.get((int) (s - 1)).getSeq() == s ? rs.get((int) (s - 1)).toLine() : "?";
                    if (ref == null) ref = linha; else if (!ref.equals(linha)) igual = false;
                }
                if (!igual) difs.add(s);
            }
            seqDivergentesTotal += difs.size();
            System.out.println("p" + pe.getKey() + ": ultimoSeq por nó:" + sb + "; comparados até seq " + minUltimo
                    + " -> " + (difs.isEmpty() ? "todos iguais [OK]" : difs.size() + " seq divergente(s) " + difs));
        }
    }

    // ------------------------------------------------------------------ resumo para as tabelas

    private static void resumo() {
        System.out.println("\n=== Resumo (para as tabelas do ponto 11) ===");
        System.out.println("Tempo até ao 1.º líder (mín / méd / máx, s): " + mmm(tempos1oLider));
        System.out.println("Termos até ao 1.º líder (por partição): " + termos1oLider);
        System.out.println("Termos sem líder: " + termosSemLiderTotal + " (votos divididos: " + votosDivididosTotal + ")");
        System.out.println("Indisponibilidade após falha (mín / méd / máx, s): " + mmm(indisponibilidades));
        System.out.println("Leituras perdidas: durante a eleição " + perdidasEleicao + " / por truncagem " + perdidasTruncagem);
        System.out.println("Registos truncados: " + truncadosTotal);
        System.out.println("Termos com mais de um líder: " + termosDoisLideres);
        System.out.println("seq divergentes na coincidência final: " + seqDivergentesTotal);
    }

    private static String mmm(List<Long> v) {
        if (v.isEmpty()) return "-";
        long mn = Long.MAX_VALUE, mx = Long.MIN_VALUE, s = 0;
        for (long x : v) { mn = Math.min(mn, x); mx = Math.max(mx, x); s += x; }
        return String.format("%.1f / %.1f / %.1f", mn / 1000.0, s / 1000.0 / v.size(), mx / 1000.0);
    }

    private static String nos(TreeSet<Integer> s) {
        StringBuilder sb = new StringBuilder("[");
        for (int x : s) { if (sb.length() > 1) sb.append(","); sb.append("n").append(x); }
        return sb.append("]").toString();
    }

    private RaftCheck() { }
}