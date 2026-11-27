# tpf-lider - LeaderBot (Sprint E2-01)

Bot líder em Java (JDA) que recebe os comandos do Discord no canal `#arquivo` e responde com mensagens provisórias (as funcionalidades reais ficam para os próximos sprints).

## Requisitos
- Java 17 ou superior (`java -version`)
- A biblioteca JDA já está incluída em `lib/JDA-6.7.0-withDependencies.jar`
- No Discord Developer Portal → **Bot**: o **Message Content Intent** tem de estar ativo
- No servidor: canal de texto `#arquivo` e cargo `admin-arquivo`

## 1. Configurar o token
1. Discord Developer Portal → a aplicação → **Bot** → copiar o token.
2. Criar o ficheiro `Sprints/E2-tpf-lider/token.txt` (ao lado das pastas `src` e `lib`).
3. Colar **apenas** o token lá dentro, sem aspas nem espaços.

O `token.txt` (e o futuro `master.key`) estão no `.gitignore` e **nunca** vão para o repositório.

## 2. Arrancar o líder
Os comandos executam-se a partir da **raiz do repositório** (`SD-2026-2027`), porque o programa procura o token em `Sprints/E2-tpf-lider/token.txt`.

**Compilar:**
```
javac -cp "Sprints/E2-tpf-lider/lib/JDA-6.7.0-withDependencies.jar" -d Sprints/E2-tpf-lider/bin Sprints/E2-tpf-lider/src/tpf/LeaderBot.java
```

**Executar (Windows):**
```
java -cp "Sprints/E2-tpf-lider/bin;Sprints/E2-tpf-lider/lib/JDA-6.7.0-withDependencies.jar" tpf.LeaderBot
```

**Executar (Linux/macOS):** igual, mas com `:` em vez de `;` no `-cp`.

Se tudo correr bem, a consola mostra:
```
O LeaderBot acordou e está online!
Comandos registados no servidor <nome do servidor>!
```
e o bot aparece online no servidor.

## 3. Comandos (só funcionam no canal #arquivo)
| Comando | Parâmetros | Quem pode usar |
|---|---|---|
| `/registar` | `texto` (máx. 1500 caracteres) | todos |
| `/estado` | — | todos |
| `/recuperar` | `de`, `ate` (≥ 0, com `de` ≤ `ate`) | todos |
| `/config r` | `n` (≥ 1) | `admin-arquivo` |
| `/replica-token` | — | `admin-arquivo` |
| `/replica-remover` | `id` | `admin-arquivo` |
| `/rajada` | `n` (≥ 1), `intervalo_ms` (≥ 0) | `admin-arquivo` |

O parâmetro da issue `intervaloMs` chama-se `intervalo_ms`, porque o Discord só aceita nomes de parâmetros em minúsculas.

As mensagens escritas no `#arquivo` (exceto as de bots) são detetadas e mostradas na consola do líder; as mensagens de outros canais são ignoradas.

## 4. Questões de Reflexão 


1. `Porque é que o token é necessário, se a réplica já se autentica com Ed25519? O que acontece se um token for intercetado nos 10 minutos de validade?`

A autenticação Ed25519 prova apenas que o cliente possui a chave privada correspondente à chave pública que apresentou, mas não prova que essa réplica está autorizada pelo administrador a entrar no sistema. Sem o token, qualquer atacante poderia gerar um par de chaves Ed25519 e registar-se livremente. 
  O token de uso único funciona como autorização de entrada para o primeiro registo.
Se um token for intercetado durante os 10 minutos de validade e antes de ser usado, um atacante pode usá-lo para registar a sua própria chave pública antes da réplica legítima (que depois verá o token recusado por já ter sido consumido). O impacto fica limitado à janela de 10 minutos e a uma única réplica, e o administrador pode revogá-la de imediato com /replica-remover <id>.

2. `Porque é que a réplica assina o id e um prefixo juntamente com o nonce, e não apenas o nonce?`

Incluir o prefixo de domínio ("tpf-registo|") impede ataques de confusão entre protocolos (cross-protocol replay), garantindo que uma assinatura produzida neste desafio não pode ser reutilizada noutro contexto ou mensagem que use a mesma chave. Incluir o id vincula criptograficamente a identidade declarada ao desafio daquela sessão, impedindo que uma assinatura válida para um id seja reaproveitada ou substituída por outro id durante o registo.

3. `Porque é que o líder usa o id autenticado da ligação, e não o <id> escrito no ACK?`

O campo <id> dentro do payload do ACK é texto enviado livremente pelo cliente e pode ser forjado ,uma réplica comprometida podia enviar um ACK com o id de outra réplica para falsificar cópias ou enganar o catálogo. O id associado à ligação WebSocket foi verificado criptograficamente por Ed25519 durante o desafio-resposta e mantido no servidor; ao exigir que o <id> do ACK coincida com o id autenticado da ligação, impede-se a falsificação de identidade.