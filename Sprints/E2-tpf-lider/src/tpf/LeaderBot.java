package tpf;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
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

            // 3. Registar os comandos na API do Discord
            jda.updateCommands().addCommands(
                    Commands.slash("estado", "Mostra o estado do sistema (Público)"),
                    Commands.slash("config", "Configura o sistema (Apenas Admins)")
            ).queue();
            System.out.println("Comandos registados com sucesso!");

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
        
        // Lógica do comando /config (Com proteção de Cargo/Role)
        else if (event.getName().equals("config")) {
            // Verifica se quem chamou o comando tem o cargo "admin-arquivo"
            boolean isAdmin = event.getMember().getRoles().stream()
                    .anyMatch(role -> role.getName().equals("admin-arquivo"));

            if (isAdmin) {
                event.reply("Comando recebido: Configurar (Ainda não implementado)").queue();
            } else {
                event.reply("Erro: Não tens permissão (admin-arquivo) para usar este comando.").queue();
            }
        }
    }
}