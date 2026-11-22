package tpf;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.interactions.commands.build.OptionData;
import net.dv8tion.jda.api.interactions.commands.build.SubcommandData;
import net.dv8tion.jda.api.requests.GatewayIntent;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import java.security.GeneralSecurityException;
import java.time.Instant;

public class LeaderBot extends ListenerAdapter {

    private static final Path PASTA_LIDER = Path.of("Sprints/E2-tpf-lider");
    private static final Path PASTA_SELADOS = PASTA_LIDER.resolve("selados");

    private final byte[] masterKey;
    private final RegistoLog registoLog;

    // chunk aberto e selagem
    private Chunk chunkAberto = new Chunk();
    private final ScheduledExecutorService selador = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> selagemAgendada;          // selagem por tempo do chunk aberto

    public LeaderBot(byte[] masterKey, RegistoLog registoLog) {
        this.masterKey = masterKey;
        this.registoLog = registoLog;
    }

    public static void main(String[] args) {
        try {
            // 1. Carregar ou gerar master.key ANTES de ligar ao Discord.
            // Se existir e não tiver 32 bytes, lança exceção e recusa arrancar.
            byte[] masterKey = MasterKey.carregarOuGerar(PASTA_LIDER.resolve("master.key"));
            System.out.println("master.key carregada com sucesso (" + masterKey.length + " bytes).");

            // 2. Inicializar o registo durável (lê o último LSN de registos.log)
            RegistoLog registoLog = new RegistoLog(
                    PASTA_LIDER.resolve("registos.log"),
                    PASTA_LIDER.resolve("eventos-lider.txt")
            );
            System.out.println("RegistoLog pronto. Último LSN recuperado: " + registoLog.getUltimoLsn());

            String token = Files.readString(PASTA_LIDER.resolve("token.txt")).trim();

            LeaderBot bot = new LeaderBot(masterKey, registoLog);

            JDA jda = JDABuilder.createDefault(token)
                    .enableIntents(GatewayIntent.MESSAGE_CONTENT)
                    .addEventListeners(bot)
                    .build();

            jda.awaitReady();
            System.out.println("O LeaderBot acordou e está online!");

            if (jda.getGuilds().isEmpty()) {
                System.err.println("Erro: o bot não está em nenhum servidor. Convida-o primeiro.");
                return;
            }
            Guild guild = jda.getGuilds().get(0);

            jda.updateCommands().queue();

            guild.updateCommands().addCommands(
                    Commands.slash("estado", "Mostra o estado do sistema (Público)"),
                    Commands.slash("config", "Configura o sistema (Apenas Admins)")
                            .addSubcommands(new SubcommandData("r", "Altera o fator de replicação R")
                                    .addOptions(new OptionData(OptionType.INTEGER, "n", "novo valor de R", true)
                                            .setMinValue(1))),
                    Commands.slash("registar", "Regista um texto no arquivo")
                            .addOptions(new OptionData(OptionType.STRING, "texto", "texto a registar", true)
                                    .setMaxLength(1500)),
                    Commands.slash("recuperar", "Recupera os registos de um intervalo de LSN")
                            .addOptions(
                                    new OptionData(OptionType.INTEGER, "de", "primeiro LSN", true).setMinValue(0),
                                    new OptionData(OptionType.INTEGER, "ate", "último LSN", true).setMinValue(0)),
                    Commands.slash("replica-token", "Gera um token de registo de réplica (Apenas Admins)"),
                    Commands.slash("replica-remover", "Remove uma réplica (Apenas Admins)")
                            .addOption(OptionType.STRING, "id", "identificador da réplica", true),
                    Commands.slash("rajada", "Injeta registos sintéticos (Apenas Admins)")
                            .addOptions(
                                    new OptionData(OptionType.INTEGER, "n", "número de registos", true).setMinValue(1),
                                    new OptionData(OptionType.INTEGER, "intervalo_ms", "intervalo entre registos, em ms", true).setMinValue(0))
            ).queue(
                    ok -> System.out.println("Comandos registados no servidor " + guild.getName() + "!"),
                    erro -> System.err.println("Erro ao registar comandos: " + erro.getMessage())
            );

        } catch (Exception e) {
            System.err.println("Erro fatal ao iniciar o bot: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Ponto único de entrada para registos (/registar, #arquivo e futuramente /rajada da Pessoa D).
     */
    public synchronized Registo registar(String autor, String canal, String texto) throws IOException {
        Registo r = registoLog.registar(autor, canal, texto);     // LSN + registos.log + fsync + REGISTO

        boolean primeiro = chunkAberto.vazio();
        chunkAberto.adicionar(r);
        if (chunkAberto.tamanho() >= Wire.CHUNK_MAX_BYTES) {
            selar();                                               // por tamanho: sela de imediato
        } else if (primeiro) {
            Chunk este = chunkAberto;                              // por tempo: SELAGEM_MS após o 1.º registo
            selagemAgendada = selador.schedule(() -> selarPorTempo(este),
                    Wire.SELAGEM_MS, TimeUnit.MILLISECONDS);
        }
        return r;
    }

    // corre na thread do selador, SELAGEM_MS depois do 1.º registo do chunk 'alvo'
    private synchronized void selarPorTempo(Chunk alvo) {
        try {
            if (chunkAberto != alvo) return;                       // já foi selado por tamanho
            selar();
        } catch (RuntimeException e) {
            System.err.println("Erro na selagem por tempo: " + e.getMessage());
        }
    }

    // fecha o chunk aberto; o registo seguinte abre um chunk novo
    private synchronized void selar() {
        if (chunkAberto.vazio()) return;
        if (selagemAgendada != null) {
            selagemAgendada.cancel(false);
            selagemAgendada = null;
        }
        Chunk chunk = chunkAberto;
        chunkAberto = new Chunk();                                 // depois de selado, o chunk não muda

        byte[] dados = chunk.serializar(Wire.PAD_BYTES);

        // cifrar, gravar selados/<chunkId>.bin e .hdr e só depois registar o evento SELADO.
        // Os registos já estão em registos.log (com fsync); se isto falhar, não se perdem.
        try {
            Sealed s = ChunkCrypto.seal(masterKey, dados);
            s.gravar(PASTA_SELADOS);
            registoLog.registarEvento(Instant.now().toString(), "SELADO",
                    "chunkId=" + s.chunkId() + " lsn=" + chunk.lsnDe() + "-" + chunk.lsnAte()
                            + " registos=" + chunk.getRegistos().size()
                            + " bytes=" + dados.length + " cifrado=" + s.ciphertext().length);
            System.out.println("Chunk selado: " + s.chunkId() + " LSN " + chunk.lsnDe() + "-" + chunk.lsnAte()
                    + ", " + chunk.getRegistos().size() + " registos, " + dados.length + " bytes");
        } catch (IOException | GeneralSecurityException e) {
            System.err.println("Erro ao selar o chunk LSN " + chunk.lsnDe() + "-" + chunk.lsnAte()
                    + ": " + e.getMessage());
        }
    }

    public byte[] getMasterKey() {
        return masterKey;
    }

    public RegistoLog getRegistoLog() {
        return registoLog;
    }

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {

        if (!event.getChannel().getName().equals("arquivo")) {
            event.reply("Os comandos só funcionam no canal #arquivo!").setEphemeral(true).queue();
            return;
        }

        if (event.getName().equals("estado")) {
            event.reply("Comando recebido: Mostrar estado (Ainda não implementado)").queue();
        }

        else if (event.getName().equals("config")) {
            if (!temAdmin(event)) return;
            int n = event.getOption("n").getAsInt();
            event.reply("Comando recebido: Fator de replicação R = " + n + " (Ainda não implementado)").queue();
        }

        // /registar <texto> — grava com fsync ANTES de responder
        else if (event.getName().equals("registar")) {
            String texto = event.getOption("texto").getAsString();
            String autor = event.getUser().getName();
            String canal = event.getChannel().getName();

            try {
                Registo r = registar(autor, canal, texto);
                event.reply("Registado com LSN " + r.lsn()).queue();
            } catch (IOException e) {
                System.err.println("Erro ao gravar registo: " + e.getMessage());
                event.reply("Erro interno ao gravar o registo em disco.").setEphemeral(true).queue();
            }
        }

        else if (event.getName().equals("recuperar")) {
            long de = event.getOption("de").getAsLong();
            long ate = event.getOption("ate").getAsLong();
            if (de > ate) {
                event.reply("Erro: o LSN 'de' (" + de + ") é maior do que o LSN 'ate' (" + ate + ").")
                        .setEphemeral(true).queue();
                return;
            }
            event.reply("Comando recebido: Recuperar os registos de LSN " + de + " a " + ate
                    + " (Ainda não implementado)").queue();
        }

        else if (event.getName().equals("replica-token")) {
            if (!temAdmin(event)) return;
            event.reply("Comando recebido: Gerar token de réplica (Ainda não implementado)")
                    .setEphemeral(true).queue();
        }

        else if (event.getName().equals("replica-remover")) {
            if (!temAdmin(event)) return;
            String id = event.getOption("id").getAsString();
            event.reply("Comando recebido: Remover a réplica " + id + " (Ainda não implementado)").queue();
        }

        else if (event.getName().equals("rajada")) {
            if (!temAdmin(event)) return;
            int n = event.getOption("n").getAsInt();
            int intervalo = event.getOption("intervalo_ms").getAsInt();
            event.reply("Comando recebido: Rajada de " + n + " registos com intervalo de " + intervalo
                    + " ms (Ainda não implementado)").queue();
        }
    }

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        if (event.getAuthor().isBot()) return;
        if (!event.isFromGuild()) return;
        if (!event.getChannel().getName().equals("arquivo")) return;

        String autor = event.getAuthor().getName();
        String canal = event.getChannel().getName();
        String texto = event.getMessage().getContentRaw();

        try {
            Registo r = registar(autor, canal, texto);
            event.getMessage().reply("Registado com LSN " + r.lsn()).queue();
        } catch (IOException e) {
            System.err.println("Erro ao gravar mensagem de #arquivo: " + e.getMessage());
        }
    }

    private boolean temAdmin(SlashCommandInteractionEvent event) {
        boolean isAdmin = event.getMember().getRoles().stream()
                .anyMatch(role -> role.getName().equals("admin-arquivo"));
        if (!isAdmin) {
            event.reply("Erro: Não tens permissão (admin-arquivo) para usar este comando.")
                    .setEphemeral(true).queue();
        }
        return isAdmin;
    }
}