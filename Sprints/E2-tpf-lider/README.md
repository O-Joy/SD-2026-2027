# tpf-lider - LeaderBot (Sprint E2-02)

Bot líder em Java (JDA). Cada `/registar` e cada mensagem do `#arquivo` recebe um LSN e é gravada em `registos.log` com fsync antes da resposta. Os registos juntam-se num chunk aberto, que é selado por tamanho ou por tempo, cifrado com AES-256-GCM e gravado em `selados/`.

`/estado`, `/config`, `/recuperar`, `/replica-token` e `/replica-remover` continuam com respostas provisórias (próximos sprints).

## Requisitos
- Java 17 ou superior (`java -version`)
- JDA em `lib/JDA-6.7.0-withDependencies.jar` (já incluído)
- Para os testes: `lib/junit-platform-console-standalone-1.11.3.jar` ([Maven Central](https://repo1.maven.org/maven2/org/junit/platform/junit-platform-console-standalone/1.11.3/junit-platform-console-standalone-1.11.3.jar))
- No Discord Developer Portal → **Bot**: o **Message Content Intent** tem de estar ativo
- No servidor: canal de texto `#arquivo` e cargo `admin-arquivo`

## 1. Configurar o token
Criar `Sprints/E2-tpf-lider/token.txt` só com o token do bot (sem aspas nem espaços).

## 2. Compilar e arrancar
Todos os comandos correm a partir da **raiz do repositório** (`SD-2026-2027`), porque o líder usa a pasta `Sprints/E2-tpf-lider/`.

**Compilar (todas as classes):**
```
javac -cp "Sprints/E2-tpf-lider/lib/JDA-6.7.0-withDependencies.jar" -d Sprints/E2-tpf-lider/bin Sprints/E2-tpf-lider/src/tpf/*.java
```

**Executar (Windows; em Linux/macOS usar `:` em vez de `;`):**
```
java -cp "Sprints/E2-tpf-lider/bin;Sprints/E2-tpf-lider/lib/JDA-6.7.0-withDependencies.jar" tpf.LeaderBot
```

**Com parâmetros alterados** (os `-D` vão **antes** do nome da classe):
```
java -Dtpf.chunkMaxBytes=8192 -Dtpf.selagemMs=2000 -cp "..." tpf.LeaderBot
```

## 3. Parâmetros (`Wire.java`)
| Constante | Valor por omissão | Propriedade `-D` | Significado |
|---|---|---|---|
| `CHUNK_MAX_BYTES` | 65536 | `tpf.chunkMaxBytes` | o chunk é selado quando os registos (sem padding) atingem este tamanho |
| `PAD_BYTES` | 4096 | `tpf.padBytes` | o chunk serializado é completado com zeros até um múltiplo deste valor |
| `SELAGEM_MS` | 5000 | `tpf.selagemMs` | o chunk é selado este tempo depois do seu 1.º registo |
| `RAJADA_TEXTO_BYTES` | 1000 | `tpf.rajadaTextoBytes` | tamanho do texto de cada registo sintético do `/rajada` |
| `MASTER_KEY_BYTES` | 32 | — | tamanho obrigatório da `master.key` |

## 4. Ficheiros gerados (em `Sprints/E2-tpf-lider/`, todos no `.gitignore`)
| Ficheiro | Conteúdo |
|---|---|
| `master.key` | 32 bytes aleatórios (`SecureRandom`), criado no 1.º arranque com permissões só do dono (`rw-------`). Se existir com outro tamanho, o líder recusa arrancar. |
| `registos.log` | uma linha por registo: `lsn;instante;autor;canal;texto` (texto em Base64). Escrito com `FileChannel.force(true)` antes de responder. No arranque, o líder continua a partir do último LSN deste ficheiro. |
| `eventos-lider.txt` | `instante;evento;detalhe`. Eventos `REGISTO` (`lsn=… autor=… canal=…`) e `SELADO` (`chunkId=… lsn=de-ate registos=… bytes=… cifrado=…`). |
| `selados/<chunkId>.bin` | texto cifrado do chunk (ver §6) |
| `selados/<chunkId>.hdr` | cabeçalho para abrir o chunk (ver §7) |

## 5. Formato binário do chunk (antes de cifrar) — `Chunk.serializar` / `Chunk.ler`
Big-endian, escrito com `DataOutputStream`:
```
int   n                                   número de registos
n vezes:
  long  lsn                               8 bytes
  int   len + len bytes UTF-8             instante (ISO-8601)
  int   len + len bytes UTF-8             autor
  int   len + len bytes UTF-8             canal
  int   len + len bytes UTF-8             texto
bytes 0x00 (padding) até o total ser múltiplo de PAD_BYTES
```
`Chunk.ler` lê o `n` e os `n` registos e ignora o padding que vem a seguir.

Selagem: por **tamanho**, quando o registo acabado de acrescentar faz o chunk atingir `CHUNK_MAX_BYTES` (é selado logo, por isso um chunk cheio tem um pouco mais de 64 KiB e fica com 69632 bytes depois do padding); por **tempo**, `SELAGEM_MS` depois do 1.º registo (`ScheduledExecutorService`). Depois de selado, o chunk não muda e o registo seguinte abre um novo.

## 6. Cifra — `ChunkCrypto`
```
salt   = 32 bytes aleatórios por chunk
nonce  = 12 bytes aleatórios por chunk
PRK    = HMAC-SHA256(chave = salt, MK)                    HKDF extract
DEK    = HMAC-SHA256(chave = PRK, "tpf-chunk-v1" || 0x01)  HKDF expand, 1 bloco = 32 bytes
AAD    = "tpf-v1" || keyVersion (int, 4 bytes) || salt || nonce      (keyVersion = 1)
.bin   = AES-256-GCM(DEK, nonce, AAD, chunk com padding)  →  ciphertext || tag (16 bytes)
chunkId = SHA-256(.bin) em hexadecimal (64 carateres)
```
Tamanho do `.bin` = tamanho com padding + 16 (ex.: 69632 + 16 = 69648).

`open` verifica, por esta ordem, e lança exceção na 1.ª falha:
1. SHA-256 do `.bin` == `chunkId`
2. `keyVersion` conhecida
3. tag GCM (falha se mudar um byte, o salt, o nonce, a keyVersion no AAD ou a master key)

## 7. Formato do `.hdr`
Uma linha de texto UTF-8:
```
keyVersion;salt;nonce
```
`keyVersion` em decimal; `salt` (32 bytes) e `nonce` (12 bytes) em Base64. Exemplo:
```
1;q8Zr…(44 carateres)…=;3vT1…(16 carateres)
```
Os ficheiros são gravados primeiro como `.tmp`, com fsync, e depois renomeados (`ATOMIC_MOVE`), para nunca ficar em `selados/` um ficheiro com o nome final incompleto.

## 8. Comandos (só no canal #arquivo)
| Comando | Parâmetros | Quem pode usar |
|---|---|---|
| `/registar` | `texto` (máx. 1500 carateres) | todos |
| `/rajada` | `n` (≥ 1), `intervalo_ms` (≥ 0) | `admin-arquivo` |
| `/estado` | — | todos |
| `/recuperar` | `de`, `ate` (≥ 0, `de` ≤ `ate`) | todos |
| `/config r` | `n` (≥ 1) | `admin-arquivo` |
| `/replica-token` | — | `admin-arquivo` |
| `/replica-remover` | `id` | `admin-arquivo` |

O `intervaloMs` da issue chama-se `intervalo_ms` porque o Discord só aceita nomes de parâmetros em minúsculas.

`/registar`, as mensagens do `#arquivo` e o `/rajada` passam todos pelo mesmo método `synchronized LeaderBot.registar(autor, canal, texto)`. Mensagens de bots e de outros canais são ignoradas.

`/rajada` responde logo (o Discord só espera 3 s) e injeta os `n` registos numa thread própria, para não bloquear a thread de eventos do JDA nem a thread do `selador`. No fim escreve no canal o intervalo de LSN.

## 9. Testes (`test/tpf/ChunkCryptoTest.java`)
```
javac -cp "Sprints/E2-tpf-lider/bin;Sprints/E2-tpf-lider/lib/junit-platform-console-standalone-1.11.3.jar" -d Sprints/E2-tpf-lider/bin-test Sprints/E2-tpf-lider/test/tpf/*.java
java -jar Sprints/E2-tpf-lider/lib/junit-platform-console-standalone-1.11.3.jar -cp "Sprints/E2-tpf-lider/bin;Sprints/E2-tpf-lider/bin-test" --select-class tpf.ChunkCryptoTest
```
(No IntelliJ: botão verde ao lado de `class ChunkCryptoTest`.) Cobrem: round-trip (memória e disco), padding múltiplo de 4096, sem texto em claro, byte alterado no `.bin`, salt / nonce / keyVersion diferentes e outra master key.

## 10. Verificar os critérios (QA)
1. `/registar ola` → responde `Registado com LSN x` e a linha `x;…` já está em `registos.log`.
2. Parar e voltar a arrancar → o próximo LSN é `x+1`.
3. Um registo → ~5 s depois aparece `SELADO` em `eventos-lider.txt` e um par `.bin`/`.hdr` em `selados/`.
4. `/rajada 500 0` → vários `SELADO` com `bytes=69632` (múltiplo de 4096) e um último, mais pequeno, por tempo.
5. `grep -r rajada Sprints/E2-tpf-lider/selados/` → não encontra nada. No Git Bash: `sha256sum selados/<id>.bin` dá o próprio `<id>`.
6. `java -cp "Sprints/E2-tpf-lider/bin" tpf.AbrirSelados` → abre todos os chunks e mostra os registos de cada intervalo de LSN do evento `SELADO`.