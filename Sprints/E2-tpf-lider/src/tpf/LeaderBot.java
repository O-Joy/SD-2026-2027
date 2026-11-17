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

import java.nio.file.Files;
import java.nio.file.Path;

// 1. A nossa classe agora "estende" (herda) o ListenerAdapter para poder ouvir eventos
public class LeaderBot extends ListenerAdapter {

    public static void main(String[] args) {
        try {
            String token = Files.readString(Path.of("Sprints/E2-tpf-lider/token.txt")).trim();

            JDA jda = JDABuilder.createDefault(token)
                    .enableIntents(GatewayIntent.MESSAGE_CONTENT)
                    // 2. Dizemos ao bot que os eventos vão ser tratados por esta própria classe
                    .addEventListeners(new LeaderBot())
                    .build();

            jda.awaitReady();
            System.out.println("O LeaderBot acordou e está online!");

            // 3. Registar os comandos no servidor de testes (comandos de guild: aparecem logo)
            if (jda.getGuilds().isEmpty()) {
                System.err.println("Erro: o bot não está em nenhum servidor. Convida-o primeiro.");
                return;
            }
            Guild guild = jda.getGuilds().get(0);

            // Apaga os comandos globais antigos (versão anterior), para não aparecerem repetidos
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
            System.err.println("Erro ao iniciar o bot: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // 4. Esta função "dispara" automaticamente sempre que alguém usa um comando no Discord
    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {

        // Verifica se o comando foi usado no canal #arquivo
        if (!event.getChannel().getName().equals("arquivo")) {
            event.reply("Os comandos só funcionam no canal #arquivo!").setEphemeral(true).queue();
            return;
        }

        // Lógica do comando /estado
        if (event.getName().equals("estado")) {
            event.reply("Comando recebido: Mostrar estado (Ainda não implementado)").queue();
        }

        // Lógica do comando /config r <n> (Com proteção de Cargo/Role)
        else if (event.getName().equals("config")) {
            if (!temAdmin(event)) return;
            int n = event.getOption("n").getAsInt();
            event.reply("Comando recebido: Fator de replicação R = " + n + " (Ainda não implementado)").queue();
        }

        // Lógica do comando /registar <texto>
        else if (event.getName().equals("registar")) {
            String texto = event.getOption("texto").getAsString();
            event.reply("Comando recebido: Registar \"" + texto + "\" (Ainda não implementado)").queue();
        }

        // Lógica do comando /recuperar <de> <ate>
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

        // Lógica do comando /replica-token (Com proteção de Cargo/Role)
        else if (event.getName().equals("replica-token")) {
            if (!temAdmin(event)) return;
            // ephemeral: o token só deve ser visto por quem o pediu
            event.reply("Comando recebido: Gerar token de réplica (Ainda não implementado)")
                    .setEphemeral(true).queue();
        }

        // Lógica do comando /replica-remover <id> (Com proteção de Cargo/Role)
        else if (event.getName().equals("replica-remover")) {
            if (!temAdmin(event)) return;
            String id = event.getOption("id").getAsString();
            event.reply("Comando recebido: Remover a réplica " + id + " (Ainda não implementado)").queue();
        }

        // Lógica do comando /rajada <n> <intervalo> (Com proteção de Cargo/Role)
        else if (event.getName().equals("rajada")) {
            if (!temAdmin(event)) return;
            int n = event.getOption("n").getAsInt();
            int intervalo = event.getOption("intervalo_ms").getAsInt();
            event.reply("Comando recebido: Rajada de " + n + " registos com intervalo de " + intervalo
                    + " ms (Ainda não implementado)").queue();
        }
    }

    // 5. Esta função "dispara" sempre que é escrita uma mensagem num canal que o bot vê
    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        if (event.getAuthor().isBot()) return;                       // ignora bots (incluindo ele próprio)
        if (!event.isFromGuild()) return;                            // ignora mensagens privadas
        if (!event.getChannel().getName().equals("arquivo")) return; // ignora outros canais

        event.getChannel().sendMessage("Mensagem recebida: \"" 
            + event.getMessage().getContentRaw() + "\" (Ainda não implementado)").queue();
    }

    // Verifica se quem chamou o comando tem o cargo "admin-arquivo"; se não tiver, responde com o erro
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