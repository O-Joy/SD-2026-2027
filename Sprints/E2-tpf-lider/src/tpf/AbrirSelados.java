package tpf;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

/**
 * Ferramenta de verificação: abre todos os chunks de selados/ com a master.key e mostra os registos.
 * Corre-se a partir da raiz do repositório, como o LeaderBot:
 *   java -cp "Sprints/E2-tpf-lider/bin" tpf.AbrirSelados
 */
public final class AbrirSelados {

    public static void main(String[] args) throws Exception {
        Path pastaLider = Path.of("Sprints/E2-tpf-lider");
        Path selados = pastaLider.resolve("selados");
        byte[] mk = Files.readAllBytes(pastaLider.resolve("master.key"));

        List<Path> bins;
        try (Stream<Path> s = Files.list(selados)) {
            bins = s.filter(p -> p.getFileName().toString().endsWith(".bin")).sorted().toList();
        }
        int falhas = 0;
        for (Path bin : bins) {
            String id = bin.getFileName().toString().replace(".bin", "");
            try {
                Chunk c = Chunk.ler(ChunkCrypto.open(mk, Sealed.ler(selados, id)));
                System.out.println("OK " + id + "  LSN " + c.lsnDe() + "-" + c.lsnAte()
                        + "  (" + c.getRegistos().size() + " registos, " + Files.size(bin) + " bytes no .bin)");
                for (Registo r : c.getRegistos()) {
                    System.out.println("    " + r.lsn() + ";" + r.instante() + ";" + r.autor() + ";"
                            + r.canal() + ";" + r.texto());
                }
            } catch (Exception e) {
                falhas++;
                System.out.println("FALHA " + id + ": " + e);
            }
        }
        System.out.println(bins.size() + " chunks, " + falhas + " falhas");
    }
}
