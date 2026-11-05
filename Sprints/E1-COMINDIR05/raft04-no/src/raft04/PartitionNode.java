package raft04;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PartitionNode {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: PartitionNode <k> <i>");
            return;
        }

        int k = Integer.parseInt(args[0]); // partição (ex: 1)
        int i = Integer.parseInt(args[1]); // ID do nó (ex: 1, 2 ou 3)

        long ultimoSeq = 0;
        long ultimoTermo = 0;

        // 1. Ler o ficheiro do nó (se existir) para descobrir ultimoSeq e ultimoTermo locais
        File noFile = new File("no-p" + k + "-n" + i + ".txt");
        if (noFile.exists()) {
            try (Scanner scanner = new Scanner(noFile)) {
                String lastLine = "";
                while (scanner.hasNextLine()) {
                    String line = scanner.nextLine();
                    if (!line.isBlank()) lastLine = line;
                }
                if (!lastLine.isEmpty()) {
                    SensorRecord rec = SensorRecord.fromLine(lastLine, Wire.GRUPO);
                    ultimoSeq = rec.getSeq();
                    ultimoTermo = rec.getTerm();
                }
            }
        }

        // 2. Ficheiros de estado e log de eventos
        Path stateFile = Path.of("estado-p" + k + "-n" + i + ".txt");
        PrintWriter eventosLog = new PrintWriter(new FileWriter("eventos-p" + k + "-n" + i + ".txt", true), true);

        // 3. Criar o estado Raft
        RaftState raft = new RaftState(k, i, Wire.GRUPO, Wire.ELEICAO_MIN_MS, Wire.ELEICAO_MAX_MS, stateFile, 
            msg -> {
                String logLine = System.currentTimeMillis() + ";" + msg;
                System.out.println(logLine);
                eventosLog.println(logLine);
            }
        );

        // 4. Abrir servidor TCP de recuperação
        int tcpPort = Wire.nodePort(k, i);
        new Thread(() -> {
            try (ServerSocket server = new ServerSocket(tcpPort)) {
                // O loop de aceitar ligações TCP fica aqui (A Pessoa B ou C adapta a recuperação)
                while (true) {
                    server.accept(); 
                    // Lógica de recuperação omitida no esqueleto (reutilizar COMINDIR02/04)
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }).start();

        // 5. Ligar ao IPFS PubSub
        IpfsPubSub ps = new IpfsPubSub("http://127.0.0.1:5001/api/v0");

        // 6. Thread de Verificação (chama tick() a cada VERIFICACAO_MS)
        final long uSeq = ultimoSeq;
        final long uTermo = ultimoTermo;
        
        ScheduledExecutorService verificador = Executors.newSingleThreadScheduledExecutor();
        verificador.scheduleAtFixedRate(() -> {
            if (Wire.isolated(k, i)) return; // Simulação de partição de rede

            long t = raft.tick();
            if (t != -1) {
                // Prazo expirou, o nó passou a candidato! Publicar PEDIDO_VOTO
                VoteRequest vr = new VoteRequest(t, i, uSeq, uTermo);
                try {
                    ps.publish(Wire.topicElection(k), vr.encode());
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);

        // 7. Subscrever tópicos (Eleição, Dados e In)
        ps.subscribe(Wire.topicElection(k), msg -> {
            if (Wire.isolated(k, i)) return;

            if (msg.startsWith("PEDIDO_VOTO")) {
                VoteRequest vr = VoteRequest.decode(msg);
                if (vr.candidate == i) return; // Ignorar as próprias mensagens

                RaftState.Vote voto = raft.onVoteRequest(vr.term, vr.candidate, vr.lastSeq, vr.lastTerm, uSeq, uTermo);
                VoteReply rep = new VoteReply(voto.term, i, vr.candidate, voto.granted);
                try {
                    ps.publish(Wire.topicElection(k), rep.encode());
                } catch (Exception e) { }
            } 
            else if (msg.startsWith("VOTO")) {
                VoteReply rep = VoteReply.decode(msg);
                if (rep.voter == i || rep.candidate != i) return; // Ignorar se não for para mim

                boolean won = raft.onVote(rep.term, rep.voter, rep.candidate, rep.granted);
                if (won) {
                    // PESSOA B: Implementar a passagem a líder aqui (inicializar seq, L, V e publicar LOTEs)
                    System.out.println("SOU LIDER! A Pessoa B tratará disto.");
                }
            }
        });

        ps.subscribe(Wire.topicData(k), msg -> {
            if (Wire.isolated(k, i)) return;
            // PESSOA B: Tratar os Lotes (onBatch, aplicar registos, pedir recuperação, truncar divergências)
        });

        ps.subscribe(Wire.topicIn(k), msg -> {
            // PESSOA B: Tratar as leituras IN (se for líder)
        });
    }
}