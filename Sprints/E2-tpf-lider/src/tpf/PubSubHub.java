package tpf;

import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.spec.X509EncodedKeySpec;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

public class PubSubHub extends WebSocketServer {

    private enum EstadoLigacao {
        NOVA, DESAFIADA, AUTENTICADA
    }

    private static class InfoLigacao {
        EstadoLigacao estado = EstadoLigacao.NOVA;
        String replicaId;
        String nonceB64;
        String chavePendenteB64;
        String tokenPendente;
        boolean primeiroRegisto;
    }

    private static class InfoToken {
        final long criadoEmMs;
        boolean usado;

        InfoToken(long criadoEmMs) {
            this.criadoEmMs = criadoEmMs;
            this.usado = false;
        }
    }

    private final Path replicasPath;
    private final RegistoLog registoLog;
    private final BiConsumer<String, String> handlerLider; // (idAutenticado, mensagem)
    private final SecureRandom random = new SecureRandom();

    // Campos mínimos exigidos na especificação
    private final Map<String, Set<WebSocket>> subscricoes = new ConcurrentHashMap<>();
    private final Map<String, InfoToken> tokens = new ConcurrentHashMap<>();

    // Réplicas registadas (id -> chavePublicaB64) e ligações ativas
    private final Map<String, String> replicasRegistadas = new ConcurrentHashMap<>();
    private final Map<WebSocket, InfoLigacao> ligacoes = new ConcurrentHashMap<>();

    public PubSubHub(int porto, Path replicasPath, RegistoLog registoLog,
                     BiConsumer<String, String> handlerLider) throws IOException {
        super(new InetSocketAddress(porto));
        this.replicasPath = replicasPath;
        this.registoLog = registoLog;
        this.handlerLider = handlerLider;
        setReuseAddr(true);
        carregarReplicas();
    }

    // -------------------------------------------------------------------------
    // Gestão de Tokens e Réplicas (/replica-token e /replica-remover)
    // -------------------------------------------------------------------------

    public synchronized String gerarToken() {
        byte[] bytes = new byte[24];
        random.nextBytes(bytes);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        tokens.put(token, new InfoToken(System.currentTimeMillis()));
        return token;
    }

    public synchronized boolean removerReplica(String id) throws IOException {
        String removida = replicasRegistadas.remove(id);
        if (removida == null) {
            return false;
        }
        gravarReplicasAtomico();

        // Desliga quaisquer ligações ativas dessa réplica
        for (Map.Entry<WebSocket, InfoLigacao> entry : new ArrayList<>(ligacoes.entrySet())) {
            if (id.equals(entry.getValue().replicaId)) {
                entry.getKey().close(1000, "Réplica removida pelo administrador");
            }
        }
        return true;
    }

    public boolean replicaExiste(String id) {
        return replicasRegistadas.containsKey(id);
    }

    // -------------------------------------------------------------------------
    // Publicação por tópico (líder -> réplicas)
    // -------------------------------------------------------------------------

    public void publish(String topico, String mensagem) {
        Set<WebSocket> destinatarios = subscricoes.get(topico);
        if (destinatarios == null || destinatarios.isEmpty()) return;

        for (WebSocket conn : destinatarios) {
            if (conn.isOpen()) {
                conn.send(mensagem);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Callbacks do WebSocketServer
    // -------------------------------------------------------------------------

    @Override
    public void onStart() {
        System.out.println("PubSubHub WebSocket à escuta no porto " + getPort() + ".");
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake) {
        ligacoes.put(conn, new InfoLigacao());
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote) {
        ligacoes.remove(conn);
        for (Set<WebSocket> set : subscricoes.values()) {
            set.remove(conn);
        }
    }

    @Override
    public void onError(WebSocket conn, Exception ex) {
        registarErro("WebSocket error: " + ex.getMessage());
    }

    @Override
    public void onMessage(WebSocket conn, String message) {
        InfoLigacao info = ligacoes.get(conn);
        if (info == null) {
            desligarComErro(conn, "Ligação desconhecida");
            return;
        }

        if (message == null || message.isBlank()) {
            desligarComErro(conn, "Mensagem vazia");
            return;
        }

        // Split com limite 3 porque PUB;<topico>;<mensagem> contém ';' dentro da mensagem
        String[] cabecalho = message.split(";", 3);
        String comando = cabecalho[0];

        switch (comando) {
            case "OLA"   -> tratarOla(conn, info, message);
            case "PROVA" -> tratarProva(conn, info, message);
            case "SUB"   -> tratarSub(conn, info, cabecalho);
            case "PUB"   -> tratarPub(conn, info, cabecalho);
            default      -> desligarComErro(conn, "Comando desconhecido: " + comando);
        }
    }

    // -------------------------------------------------------------------------
    // Máquina de Estados: NOVA -> DESAFIADA -> AUTENTICADA
    // -------------------------------------------------------------------------

    private synchronized void tratarOla(WebSocket conn, InfoLigacao info, String message) {
        if (info.estado != EstadoLigacao.NOVA) {
            desligarComErro(conn, "OLA fora de ordem (estado=" + info.estado + ")");
            return;
        }

        String[] partes = message.split(";");
        if (partes.length != 2 && partes.length != 4) {
            desligarComErro(conn, "Formato OLA inválido");
            return;
        }

        String id = partes[1].trim();
        if (id.isEmpty()) {
            desligarComErro(conn, "ID de réplica vazio no OLA");
            return;
        }

        String chaveExistente = replicasRegistadas.get(id);

        if (partes.length == 4) {
            // 1.º registo: OLA;<id>;<chavePublicaB64>;<token>
            String pubKeyB64 = partes[2].trim();
            String token = partes[3].trim();

            if (chaveExistente != null) {
                if (!chaveExistente.equals(pubKeyB64)) {
                    desligarComErro(conn, "OLA com id já registado (" + id + ") e chave diferente");
                    return;
                }
                // Já registada com a mesma chave — não consome token
                info.primeiroRegisto = false;
                info.chavePendenteB64 = chaveExistente;
            } else {
                if (!tokenValido(token)) {
                    desligarComErro(conn, "Token inválido, já usado ou expirado para " + id);
                    return;
                }
                if (!chaveEd25519Valida(pubKeyB64)) {
                    desligarComErro(conn, "Chave pública Ed25519 inválida no OLA de " + id);
                    return;
                }
                info.primeiroRegisto = true;
                info.chavePendenteB64 = pubKeyB64;
                info.tokenPendente = token;
            }
        } else {
            // Ligações seguintes: OLA;<id>
            if (chaveExistente == null) {
                desligarComErro(conn, "Réplica não registada sem token: " + id);
                return;
            }
            info.primeiroRegisto = false;
            info.chavePendenteB64 = chaveExistente;
        }

        // Nonce de 32 bytes único para esta ligação
        byte[] nonce = new byte[32];
        random.nextBytes(nonce);
        info.replicaId = id;
        info.nonceB64 = Base64.getEncoder().encodeToString(nonce);
        info.estado = EstadoLigacao.DESAFIADA;

        conn.send("DESAFIO;" + info.nonceB64);
    }

    private synchronized void tratarProva(WebSocket conn, InfoLigacao info, String message) {
        if (info.estado != EstadoLigacao.DESAFIADA || info.nonceB64 == null) {
            desligarComErro(conn, "PROVA sem desafio ativo (estado=" + info.estado + ")");
            return;
        }

        // Descarta o nonce imediatamente — só serve para uma tentativa
        String nonceUsado = info.nonceB64;
        info.nonceB64 = null;

        String[] partes = message.split(";");
        if (partes.length != 3) {
            desligarComErro(conn, "Formato PROVA inválido");
            return;
        }

        String id = partes[1].trim();
        String assinaturaB64 = partes[2].trim();

        if (!id.equals(info.replicaId)) {
            desligarComErro(conn, "ID na PROVA (" + id + ") diferente do OLA (" + info.replicaId + ")");
            return;
        }

        if (!verificarProva(id, nonceUsado, assinaturaB64, info.chavePendenteB64)) {
            desligarComErro(conn, "Assinatura Ed25519 inválida na PROVA de " + id);
            return;
        }

        // Só consome o token e grava em replicas.txt DEPOIS da PROVA válida
        if (info.primeiroRegisto) {
            if (!tokenValido(info.tokenPendente)) {
                desligarComErro(conn, "Token expirou ou foi usado antes da PROVA de " + id);
                return;
            }
            String existente = replicasRegistadas.get(id);
            if (existente != null && !existente.equals(info.chavePendenteB64)) {
                desligarComErro(conn, "Conflito de registo concorrente para " + id);
                return;
            }

            tokens.get(info.tokenPendente).usado = true;
            replicasRegistadas.put(id, info.chavePendenteB64);
            try {
                gravarReplicasAtomico();
            } catch (IOException e) {
                desligarComErro(conn, "Erro ao gravar replicas.txt: " + e.getMessage());
                return;
            }
        }

        info.estado = EstadoLigacao.AUTENTICADA;
        System.out.println("Réplica autenticada com sucesso: " + id);
    }

    private void tratarSub(WebSocket conn, InfoLigacao info, String[] cabecalho) {
        if (info.estado != EstadoLigacao.AUTENTICADA) {
            desligarComErro(conn, "SUB antes da autenticação");
            return;
        }
        if (cabecalho.length < 2) {
            desligarComErro(conn, "Formato SUB inválido");
            return;
        }

        String topico = cabecalho[1].trim();
        String topicoProprio = "tpf-r-" + info.replicaId;

        if (!topico.equals(topicoProprio) && !topico.equals("tpf-ctl")) {
            desligarComErro(conn, "Subscrição não autorizada de " + info.replicaId + " em " + topico);
            return;
        }

        subscricoes.computeIfAbsent(topico, k -> ConcurrentHashMap.newKeySet()).add(conn);
    }

    private void tratarPub(WebSocket conn, InfoLigacao info, String[] cabecalho) {
        if (info.estado != EstadoLigacao.AUTENTICADA) {
            desligarComErro(conn, "PUB antes da autenticação");
            return;
        }
        if (cabecalho.length < 3) {
            desligarComErro(conn, "Formato PUB inválido");
            return;
        }

        String topico = cabecalho[1].trim();
        String payload = cabecalho[2];

        if (!topico.equals("tpf-lider")) {
            desligarComErro(conn, "Publicação não autorizada de " + info.replicaId + " em " + topico);
            return;
        }

        // Entrega ao líder em processo juntamente com o id autenticado da ligação
        if (handlerLider != null) {
            handlerLider.accept(info.replicaId, payload);
        }
    }

    // -------------------------------------------------------------------------
    // Criptografia Ed25519 (apenas JDK)
    // -------------------------------------------------------------------------

    public boolean verificarProva(String id, String nonceB64, String assinaturaB64, String chavePublicaB64) {
        try {
            byte[] keyBytes = Base64.getDecoder().decode(chavePublicaB64);
            byte[] sigBytes = Base64.getDecoder().decode(assinaturaB64);

            KeyFactory kf = KeyFactory.getInstance("Ed25519");
            PublicKey pubKey = kf.generatePublic(new X509EncodedKeySpec(keyBytes));

            String dados = "tpf-registo|" + id + "|" + nonceB64;
            Signature sig = Signature.getInstance("Ed25519");
            sig.initVerify(pubKey);
            sig.update(dados.getBytes(StandardCharsets.UTF_8));
            return sig.verify(sigBytes);
        } catch (Exception e) {
            return false;
        }
    }

    private boolean chaveEd25519Valida(String chavePublicaB64) {
        try {
            byte[] keyBytes = Base64.getDecoder().decode(chavePublicaB64);
            KeyFactory kf = KeyFactory.getInstance("Ed25519");
            kf.generatePublic(new X509EncodedKeySpec(keyBytes));
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private boolean tokenValido(String token) {
        if (token == null || token.isBlank()) return false;
        InfoToken info = tokens.get(token);
        if (info == null || info.usado) return false;
        long idadeMs = System.currentTimeMillis() - info.criadoEmMs;
        return idadeMs <= Wire.TOKEN_VALIDADE_MS;
    }

    // -------------------------------------------------------------------------
    // Persistência atómica de replicas.txt e registo de erros
    // -------------------------------------------------------------------------

    private void carregarReplicas() throws IOException {
        if (!Files.exists(replicasPath)) return;
        List<String> linhas = Files.readAllLines(replicasPath, StandardCharsets.UTF_8);
        for (String linha : linhas) {
            String limpa = linha.trim();
            if (limpa.isEmpty()) continue;
            String[] partes = limpa.split(";", 2);
            if (partes.length == 2) {
                replicasRegistadas.put(partes[0].trim(), partes[1].trim());
            }
        }
    }

    private void gravarReplicasAtomico() throws IOException {
        Path parent = replicasPath.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        Path tmp = replicasPath.resolveSibling(replicasPath.getFileName() + ".tmp");

        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : replicasRegistadas.entrySet()) {
            sb.append(entry.getKey()).append(";").append(entry.getValue()).append("\n");
        }

        Files.writeString(tmp, sb.toString(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);

        try {
            Files.move(tmp, replicasPath, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(tmp, replicasPath, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private void desligarComErro(WebSocket conn, String motivo) {
        registarErro(motivo);
        if (conn != null && conn.isOpen()) {
            conn.close(1008, motivo);
        }
    }

    private void registarErro(String motivo) {
        System.err.println("[PubSubHub ERRO] " + motivo);
        try {
            if (registoLog != null) {
                registoLog.registarEvento(Instant.now().toString(), "ERRO", motivo);
            }
        } catch (IOException e) {
            System.err.println("Falha ao escrever ERRO em eventos-lider.txt: " + e.getMessage());
        }
    }
}