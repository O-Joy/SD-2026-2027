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