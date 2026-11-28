import assert from "node:assert/strict";
import { closeSync, openSync } from "node:fs";
import { mkdir } from "node:fs/promises";
import { join } from "node:path";
import { SQL } from "bun";

const evidence = process.env.EVIDENCE_DIR!;
const root = process.cwd();
const mailpitUrl = "http://127.0.0.1:8025";
const appUrl = "http://127.0.0.1:3000";
await mkdir(evidence, { recursive: true });
const database = "postgres://biblioteca:biblioteca@127.0.0.1:5432/biblioteca";
const sql = new SQL(database);
const environment = {
  ...process.env,
  DATABASE_URL: database,
  REDIS_PREFIX: "biblioteca",
  QUEUE_URL: process.env.TEST_QUEUE_URL!,
  EMAIL_PROVIDER: "",
  EMAIL_URL: mailpitUrl,
  S3_BUCKET: "biblioteca",
};
let worker: ReturnType<typeof Bun.spawn> | undefined;
let app: ReturnType<typeof Bun.spawn> | undefined;
let restartedMailpit: ReturnType<typeof Bun.spawn> | undefined;

function start(entry: string, log: string) {
  const output = openSync(join(evidence, log), "w");
  const child = Bun.spawn([process.execPath, entry], {
    cwd: join(root, "backend"), env: environment,
    stdout: output, stderr: output,
  });
  closeSync(output);
  return child;
}

async function waitFor<T>(label: string, check: () => Promise<T>, timeout = 30_000): Promise<T> {
  const end = Date.now() + timeout;
  while (Date.now() < end) {
    try { const result = await check(); if (result) return result; } catch {}
    await Bun.sleep(200);
  }
  throw new Error("Tempo esgotado: " + label);
}

async function api(path: string, data: unknown) {
  const response = await fetch(appUrl + "/api" + path, {
    method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(data),
  });
  const body = await response.json();
  assert.equal(response.status, 201, JSON.stringify(body));
  return body as any;
}

function cpf() {
  let digits = Array.from(crypto.getRandomValues(new Uint8Array(9)), n => String(n % 10)).join("");
  if (/^(\d)\1{8}$/.test(digits)) return cpf();
  for (const length of [9, 10]) {
    const sum = [...digits].reduce((total, digit, i) => total + Number(digit) * (length + 1 - i), 0);
    const rest = sum * 10 % 11;
    digits += rest === 10 ? "0" : String(rest);
  }
  return digits;
}

function isbn() {
  const twelve = "978" + Array.from(crypto.getRandomValues(new Uint8Array(9)), n => String(n % 10)).join("");
  const sum = [...twelve].reduce((total, digit, i) => total + Number(digit) * (i % 2 ? 3 : 1), 0);
  return twelve + (10 - sum % 10) % 10;
}

const address = crypto.randomUUID() + "@exemplo.com";
async function inbox() {
  return await (await fetch(mailpitUrl + "/api/v1/search?query=" + encodeURIComponent("to:" + address))).json() as any;
}
async function marked(id: number) {
  const [row] = await sql`SELECT comprovante_enviado_em AS "sentAt" FROM emprestimos WHERE id = ${id}`;
  return row.sentAt;
}
async function stop(process: ReturnType<typeof Bun.spawn>) {
  process.kill("SIGTERM");
  await process.exited;
}

try {
  app = start("src/main.ts", "app.log");
  await waitFor("app", async () => (await fetch(appUrl + "/api")).ok);
  worker = start("src/worker.ts", "worker-inicial.log");
  const author = await api("/autores", { nome: "Autor da validação da fila", tipo: "literatura" });
  const book = await api("/livros", { isbn: isbn(), titulo: "Livro da validação da fila", autorId: author.id });
  const copies = [];
  for (let i = 0; i < 3; i++) copies.push(await api("/exemplares", { livroId: book.id, localizacao: "Estante de validação", estadoConservacao: "bom" }));
  const reader = await api("/leitores", { nome: "Leitor da validação", cpf: cpf(), email: address, telefone: "27999991234" });

  const started = performance.now();
  const first = await api("/emprestimos", { leitorId: reader.id, exemplarId: copies[0].id });
  const responseMs = performance.now() - started;
  await waitFor("primeiro comprovante", async () => (await inbox()).messages_count === 1);
  assert.ok(await marked(first.id));

  await stop(worker);
  worker = undefined;
  const second = await api("/emprestimos", { leitorId: reader.id, exemplarId: copies[1].id });
  await Bun.sleep(700);
  assert.equal((await inbox()).messages_count, 1);
  assert.equal(await marked(second.id), null);
  const lagProcess = Bun.spawn(["redis-cli", "-p", "6380", "--json", "XINFO", "GROUPS", "biblioteca:circulacao:fila:comprovantes"]);
  const groups = JSON.parse(await new Response(lagProcess.stdout).text());
  await lagProcess.exited;
  await Bun.write(join(evidence, "fila-com-worker-parado.json"), JSON.stringify(groups, null, 2));
  assert.ok(JSON.stringify(groups).includes("lag"));
  worker = start("src/worker.ts", "worker-retomado.log");
  await waitFor("comprovante após retomar worker", async () => (await inbox()).messages_count === 2);
  assert.ok(await marked(second.id));

  const mailpitPid = Number((await Bun.file(join(evidence, "mailpit.pid")).text()).trim());
  process.kill(mailpitPid, "SIGTERM");
  await waitFor("Mailpit parado", async () => {
    try { await fetch(mailpitUrl + "/readyz"); return false; } catch { return true; }
  }, 5_000);
  const third = await api("/emprestimos", { leitorId: reader.id, exemplarId: copies[2].id });
  await waitFor("falha do provedor no log", async () => /falhou na tentativa 1/.test(await Bun.file(join(evidence, "worker-retomado.log")).text()), 15_000);
  assert.equal(await marked(third.id), null);
  const mailpitLog = openSync(join(evidence, "mailpit-retomado.log"), "w");
  restartedMailpit = Bun.spawn([join(process.env.RUNNER_TEMP!, "fila-bin/mailpit"), "--listen", "127.0.0.1:8025"], { stdout: mailpitLog, stderr: mailpitLog });
  closeSync(mailpitLog);
  await waitFor("Mailpit retomado", async () => (await fetch(mailpitUrl + "/readyz")).ok);
  const thirdInbox = await waitFor("nova tentativa com provedor recuperado", async () => {
    const result = await inbox();
    return result.messages.find((message: any) => message.Subject === "Comprovante de empréstimo: " + book.titulo) && await marked(third.id) ? result : false;
  });
  // Mailpit usa uma caixa em memória: reiniciar apaga as mensagens anteriores.
  assert.equal(thirdInbox.messages_count, 1);
  const message = await (await fetch(mailpitUrl + "/api/v1/message/" + thirdInbox.messages[0].ID)).json() as any;
  assert.ok(message.Text.includes("Empréstimo nº " + third.id));
  assert.ok(message.Text.includes("exemplar " + third.numeroRegistro));
  const [year, month, day] = third.dataPrevistaDevolucao.split("-");
  assert.ok(message.Text.includes("Devolva até " + day + "/" + month + "/" + year));
  await Bun.write(join(evidence, "comprovante-mailpit.json"), JSON.stringify(message, null, 2));

  const duplicate = Bun.spawn(["redis-cli", "-p", "6380", "-x", "XADD", "biblioteca:circulacao:fila:comprovantes", "*", "dados"], { stdin: "pipe" });
  duplicate.stdin.write(JSON.stringify({ loanId: third.id }));
  duplicate.stdin.end();
  assert.equal(await duplicate.exited, 0);
  await waitFor("mensagem repetida ignorada", async () => (await Bun.file(join(evidence, "worker-retomado.log")).text()).includes("Comprovante do empréstimo " + third.id + ": já enviado"));
  assert.equal((await inbox()).messages_count, 1);
  const workerLogs = (await Bun.file(join(evidence, "worker-inicial.log")).text()) + (await Bun.file(join(evidence, "worker-retomado.log")).text());
  assert.equal(workerLogs.includes(address), false);
  const report = { resultado: "aprovado", emprestimos: [first.id, second.id, third.id], respostaHttpMs: Math.round(responseMs), envioPeloWorker: true, esperaSemWorker: true, novaTentativaAposFalha: true, duplicacaoIgnorada: true, emailAusenteDosLogs: true };
  await Bun.write(join(evidence, "verificacao-worker.json"), JSON.stringify(report, null, 2));
  console.log(JSON.stringify(report, null, 2));
} finally {
  if (worker) await stop(worker);
  if (app) await stop(app);
  if (restartedMailpit) await stop(restartedMailpit);
  await sql.close();
}
