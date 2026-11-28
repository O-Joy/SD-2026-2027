package tpf;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.spec.PKCS8EncodedKeySpec;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Serviço de réplica: guarda chunks cifrados enviados pelo líder.
 *
 *   java tpf.ReplicaService <id> <url> <quotaBytes> [token]
 *   ex.: java tpf.ReplicaService r1 ws://localhost:8765 8000000 <token do /replica-token>
 *
 * Pasta dados-<id>/ (relativa à pasta de trabalho):
 *   privada.pk8          chave privada Ed25519 (PKCS#8), só o dono lê
 *   publica.x509         chave pública Ed25519 (X.509), só o dono lê
 *   registada            marca: o 1.º registo já foi aceite (as ligações seguintes enviam só OLA;<id>)
 *   <chunkId>.bin/.hdr   chunks recebidos por STORE
 * Eventos: eventos-<id>.txt (instante;EVENTO;detalhe), ao lado da pasta dados-<id>/.
 *
 * Concorrência: tudo o que mexe no estado (ligação, usados, ficheiros) corre numa única thread,
 * a do executor 'replica'. As callbacks do WebSocket (threads do HttpClient) só passam o trabalho
 * para essa thread. Assim não há duas mensagens enviadas ao mesmo tempo nem corridas sobre 'usados'.
 */
public final class ReplicaService {

    private static final Pattern ID_VALIDO = Pattern.compile("[A-Za-z0-9_-]+");
    private static final Pattern CHUNK_ID_VALIDO = Pattern.compile("[0-9a-f]{64}");
    private static final long TIMEOUT_ENVIO_MS = 5000;

    // ---- configuração ----
    private final String id;
    private final URI url;
    private final long quota;
    private final String token;              // só usado no 1.º registo
    private final Path pasta;                // dados-<id>/
    private final Path eventos;              // eventos-<id>.txt
    private final Path marcaRegistada;       // dados-<id>/registada
    private final PrivateKey privada;
    private final String publicaB64;

    // ---- estado: só lido e alterado na thread 'replica' ----
    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
            r -> new Thread(r, "replica"));
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private WebSocket ws;                    // ligação atual (null se desligada)
    private int geracao;                     // número da ligação atual: ignora callbacks de ligações antigas
    private long instanteProva = -1;         // quando foi enviada a PROVA nesta ligação (-1: ainda não)
    private boolean ligacaoConfirmada;       // a ligação sobreviveu à autenticação
    private long backoffMs = Wire.BACKOFF_MIN_MS;
    private long usados;                     // soma dos .bin da pasta

    public static void main(String[] args) throws Exception {
        if (args.length < 3 || args.length > 4) {
            System.err.println("Uso: java tpf.ReplicaService <id> <url> <quotaBytes> [token]");
            System.exit(2);
        }
        String id = args[0];
        if (!ID_VALIDO.matcher(id).matches()) {
            System.err.println("id inválido (só letras, dígitos, '-' e '_'): " + id);
            System.exit(2);
        }
        long quota = Long.parseLong(args[2]);
        String token = args.length == 4 ? args[3] : null;
        new ReplicaService(id, URI.create(args[1]), quota, token).iniciar();
    }

    public ReplicaService(String id, URI url, long quota, String token) throws IOException, GeneralSecurityException {
        this.id = id;
        this.url = url;
        this.quota = quota;
        this.token = token;
        this.pasta = Path.of("dados-" + id);
        this.eventos = Path.of("eventos-" + id + ".txt");
        this.marcaRegistada = pasta.resolve("registada");

        Files.createDirectories(pasta);
        KeyPair par = carregarOuGerarChaves();
        this.privada = par.getPrivate();
        this.publicaB64 = Base64.getEncoder().encodeToString(par.getPublic().getEncoded());
        this.usados = calcularUsados();
    }

    private void iniciar() {
        System.out.println("Réplica " + id + ": quota " + quota + " bytes, usados " + usados
                + " bytes, líder " + url);
        executor.execute(this::ligar);
        executor.scheduleAtFixedRate(this::enviarHeartbeat,
                Wire.PERIODO_HB_MS, Wire.PERIODO_HB_MS, TimeUnit.MILLISECONDS);
    }

    // =====================================================================
    // Ponto 6: chaves Ed25519 e espaço usado
    // =====================================================================

    // lê o par de chaves de dados-<id>/ ou, se não existir, cria-o (só o dono pode ler)
    private KeyPair carregarOuGerarChaves() throws IOException, GeneralSecurityException {
        Path privPath = pasta.resolve("privada.pk8");
        Path pubPath = pasta.resolve("publica.x509");
        KeyFactory kf = KeyFactory.getInstance("Ed25519");

        if (Files.exists(privPath) && Files.exists(pubPath)) {
            PrivateKey priv = kf.generatePrivate(new PKCS8EncodedKeySpec(Files.readAllBytes(privPath)));
            var pub = kf.generatePublic(new java.security.spec.X509EncodedKeySpec(Files.readAllBytes(pubPath)));
            return new KeyPair(pub, priv);
        }
        if (Files.exists(privPath) || Files.exists(pubPath)) {
            throw new IOException("falta uma das chaves em " + pasta + " (privada.pk8 / publica.x509)");
        }

        KeyPair par = KeyPairGenerator.getInstance("Ed25519").generateKeyPair();
        Files.write(privPath, par.getPrivate().getEncoded(), StandardOpenOption.CREATE_NEW);   // PKCS#8
        Files.write(pubPath, par.getPublic().getEncoded(), StandardOpenOption.CREATE_NEW);     // X.509
        soDono(privPath);
        soDono(pubPath);
        System.out.println("Par de chaves Ed25519 criado em " + pasta);
        return par;
    }

    private static void soDono(Path p) throws IOException {
        try {
            Files.setPosixFilePermissions(p, PosixFilePermissions.fromString("rw-------"));
        } catch (UnsupportedOperationException e) {     // Windows
            File f = p.toFile();
            f.setReadable(false, false);
            f.setWritable(false, false);
            f.setReadable(true, true);
            f.setWritable(true, true);
        }
    }

    // soma o tamanho de todos os .bin da pasta
    private long calcularUsados() throws IOException {
        long total = 0;
        for (Path bin : listarBins()) {
            total += Files.size(bin);
        }
        return total;
    }

    private List<Path> listarBins() throws IOException {
        try (Stream<Path> s = Files.list(pasta)) {
            return s.filter(p -> p.getFileName().toString().endsWith(".bin")).sorted().toList();
        }
    }

    // =====================================================================
    // Ponto 7: ligação, autenticação e religação com backoff exponencial
    // =====================================================================

    private void ligar() {
        int minhaGeracao = ++geracao;
        try {
            ws = http.newWebSocketBuilder()
                    .buildAsync(url, new Ouvinte(minhaGeracao))
                    .get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            ws = null;
            evento("ERRO", "ligação a " + url + " falhou: " + causa(e));
            agendarReligacao();
            return;
        }
        instanteProva = -1;
        ligacaoConfirmada = false;
        evento("LIGADO", url.toString());

        // 1.º registo: OLA com chave e token; seguintes: só OLA;<id>
        boolean primeiro = !Files.exists(marcaRegistada);
        if (primeiro && token != null) {
            enviar("OLA;" + id + ";" + publicaB64 + ";" + token);
        } else {
            if (primeiro) {
                evento("ERRO", "1.º registo sem token: o hub vai recusar (use /replica-token)");
            }
            enviar("OLA;" + id);
        }
    }

    private void agendarReligacao() {
        long espera = backoffMs;
        evento("RELIGAR", String.valueOf(espera));
        backoffMs = Math.min(backoffMs * 2, Wire.BACKOFF_MAX_MS);      // 1, 2, 4, 8, 16, 30, 30, ... s
        executor.schedule(this::ligar, espera, TimeUnit.MILLISECONDS);
    }

    // a ligação fechou ou deu erro (corre na thread 'replica')
    private void ligacaoTerminou(int g, String motivo) {
        if (g != geracao || ws == null) {
            return;                                     // callback de uma ligação antiga, ou já tratada
        }
        ws = null;
        evento("DESLIGADO", motivo);
        agendarReligacao();
    }

    // trata uma mensagem completa recebida do hub (corre na thread 'replica')
    private void tratarMensagem(int g, String msg) {
        if (g != geracao || ws == null) {
            return;
        }
        if (msg.startsWith("DESAFIO;")) {
            tratarDesafio(msg.substring("DESAFIO;".length()).trim());
        } else if (msg.startsWith("STORE;")) {
            tratarStore(msg);
        } else {
            evento("ERRO", "mensagem desconhecida: " + resumo(msg));
        }
    }

    // DESAFIO;<nonceB64> -> PROVA, SUB e INVENTARIO
    private void tratarDesafio(String nonceB64) {
        try {
            Signature s = Signature.getInstance("Ed25519");
            s.initSign(privada);
            s.update(("tpf-registo|" + id + "|" + nonceB64).getBytes(StandardCharsets.UTF_8));
            String assinatura = Base64.getEncoder().encodeToString(s.sign());

            enviar("PROVA;" + id + ";" + assinatura);
            instanteProva = System.currentTimeMillis();
            enviar("SUB;" + Wire.topicoReplica(id));
            enviar("SUB;" + Wire.TOPICO_CTL);
            enviarInventario();
        } catch (GeneralSecurityException e) {
            evento("ERRO", "assinatura falhou: " + e.getMessage());
        }
    }

    // =====================================================================
    // Ponto 8: heartbeat e inventário
    // =====================================================================

    // corre de PERIODO_HB_MS em PERIODO_HB_MS na thread 'replica'
    private void enviarHeartbeat() {
        if (ws == null || instanteProva < 0) {
            return;                                     // desligada ou ainda não autenticada
        }
        enviar("PUB;" + Wire.TOPICO_LIDER + ";HB;" + id + ";" + usados + ";" + quota);

        // Se a PROVA fosse recusada, o hub fechava a ligação logo a seguir. Se a ligação continua
        // aberta pelo menos 1 s depois da PROVA, considera-se bem-sucedida: o backoff volta ao mínimo
        // e o 1.º registo fica marcado (as ligações seguintes já não enviam chave nem token).
        if (!ligacaoConfirmada && ws != null && System.currentTimeMillis() - instanteProva >= 1000) {
            ligacaoConfirmada = true;
            backoffMs = Wire.BACKOFF_MIN_MS;
            if (!Files.exists(marcaRegistada)) {
                try {
                    Files.writeString(marcaRegistada, Instant.now() + "\n");
                } catch (IOException e) {
                    evento("ERRO", "não foi possível criar " + marcaRegistada + ": " + e.getMessage());
                }
            }
        }
    }

    private void enviarInventario() {
        try {
            String ids = listarBins().stream()
                    .map(p -> p.getFileName().toString().replace(".bin", ""))
                    .collect(Collectors.joining(","));
            enviar("PUB;" + Wire.TOPICO_LIDER + ";INVENTARIO;" + id + ";" + ids);
        } catch (IOException e) {
            evento("ERRO", "inventário: " + e.getMessage());
        }
    }

    // =====================================================================
    // Ponto 9: STORE;<chunkId>;<hdrB64>;<binB64>
    // =====================================================================

    private void tratarStore(String msg) {
        String[] p = msg.split(";", 4);
        if (p.length != 4) {
            evento("ERRO", "STORE mal formado: " + resumo(msg));
            return;
        }
        String chunkId = p[1].trim();
        if (!CHUNK_ID_VALIDO.matcher(chunkId).matches()) {    // também impede nomes de ficheiro perigosos
            evento("ERRO", "STORE com chunkId inválido: " + resumo(chunkId));
            return;
        }
        byte[] hdr;
        byte[] bin;
        try {
            hdr = Base64.getDecoder().decode(p[2].trim());
            bin = Base64.getDecoder().decode(p[3].trim());
        } catch (IllegalArgumentException e) {
            evento("ERRO", "STORE " + chunkId + " com Base64 inválido");
            return;
        }

        try {
            Path binPath = pasta.resolve(chunkId + ".bin");
            Path hdrPath = pasta.resolve(chunkId + ".hdr");

            // já existe (e está íntegro): ACK sem reescrever
            if (Files.exists(binPath) && Files.exists(hdrPath)
                    && sha256Hex(Files.readAllBytes(binPath)).equals(chunkId)) {
                evento("STORE", chunkId + " já existe: ACK sem reescrever");
                enviarAck(chunkId);
                return;
            }

            // quota
            if (usados + bin.length > quota) {
                evento("QUOTA", chunkId + " " + bin.length + " bytes não cabe (usados=" + usados
                        + " quota=" + quota + ")");
                return;
            }

            // integridade: o nome tem de ser o SHA-256 do .bin
            String sha = sha256Hex(bin);
            if (!sha.equals(chunkId)) {
                evento("ERRO", "STORE " + chunkId + " recusado: SHA-256 do .bin é " + sha);
                return;
            }

            // gravar .hdr e depois .bin (temporário + fsync + ATOMIC_MOVE); só depois o ACK
            boolean substitui = Files.exists(binPath);
            long antigo = substitui ? Files.size(binPath) : 0;
            gravarAtomico(hdrPath, hdr);
            gravarAtomico(binPath, bin);
            usados += bin.length - antigo;
            evento("STORE", chunkId + " " + bin.length + " bytes (usados=" + usados + ")");
            enviarAck(chunkId);
        } catch (IOException | GeneralSecurityException e) {
            evento("ERRO", "STORE " + chunkId + " falhou: " + e.getMessage());
        }
    }

    private void enviarAck(String chunkId) throws IOException, GeneralSecurityException {
        String sha = sha256Hex(Files.readAllBytes(pasta.resolve(chunkId + ".bin")));   // do ficheiro gravado
        enviar("PUB;" + Wire.TOPICO_LIDER + ";ACK;" + id + ";" + chunkId + ";" + sha);
    }

    private static void gravarAtomico(Path destino, byte[] bytes) throws IOException {
        Path tmp = destino.resolveSibling(destino.getFileName() + ".tmp");
        try (FileChannel ch = FileChannel.open(tmp, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING)) {
            ByteBuffer buf = ByteBuffer.wrap(bytes);
            while (buf.hasRemaining()) {
                ch.write(buf);
            }
            ch.force(true);
        }
        Files.move(tmp, destino, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static String sha256Hex(byte[] dados) throws GeneralSecurityException {
        return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(dados));
    }

    // =====================================================================
    // Envio, eventos (ponto 10) e auxiliares
    // =====================================================================

    // envia uma mensagem de texto; só é chamado na thread 'replica', por isso nunca há dois envios em curso
    private void enviar(String msg) {
        WebSocket atual = ws;
        if (atual == null) {
            return;
        }
        try {
            atual.sendText(msg, true).get(TIMEOUT_ENVIO_MS, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            evento("ERRO", "envio falhou: " + causa(e));
            atual.abort();
            ligacaoTerminou(geracao, "envio falhou");
        }
    }

    // instante;EVENTO;detalhe em eventos-<id>.txt (e na consola)
    private void evento(String tipo, String detalhe) {
        String linha = Instant.now() + ";" + tipo + ";" + detalhe;
        System.out.println(linha);
        try {
            Files.writeString(eventos, linha + "\n", StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("Falha ao escrever " + eventos + ": " + e.getMessage());
        }
    }

    private static String resumo(String s) {
        return s.length() <= 80 ? s : s.substring(0, 80) + "...";
    }

    private static String causa(Throwable e) {
        while (e.getCause() != null) {
            e = e.getCause();
        }
        return e.getClass().getSimpleName() + (e.getMessage() == null ? "" : ": " + e.getMessage());
    }

    // Callbacks do WebSocket (threads do HttpClient): só juntam os fragmentos e passam à thread 'replica'
    private final class Ouvinte implements WebSocket.Listener {
        private final int g;
        private final StringBuilder parcial = new StringBuilder();

        Ouvinte(int g) { this.g = g; }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            parcial.append(data);                    // um STORE grande pode chegar em vários fragmentos
            if (last) {
                String msg = parcial.toString();
                parcial.setLength(0);
                executor.execute(() -> tratarMensagem(g, msg));
            }
            webSocket.request(1);
            return null;
        }

        @Override
        public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
            executor.execute(() -> ligacaoTerminou(g, "código " + statusCode
                    + (reason == null || reason.isEmpty() ? "" : " (" + reason + ")")));
            return null;
        }

        @Override
        public void onError(WebSocket webSocket, Throwable error) {
            executor.execute(() -> ligacaoTerminou(g, "erro: " + causa(error)));
        }
    }
}
