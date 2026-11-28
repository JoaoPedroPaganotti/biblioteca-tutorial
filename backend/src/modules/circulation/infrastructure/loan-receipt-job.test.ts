import { afterAll, expect, test } from "bun:test";
import { api, testCopy } from "#tests/api";
import { testReader } from "#tests/reader-fixtures";
import { createMemorySender, emailSender, type MemorySender } from "../../../infrastructure/email";
import { sql } from "../../../infrastructure/postgres";
import { prepareGroups, workOnce } from "../../../infrastructure/queue";
import { createRedis } from "../../../infrastructure/redis";
import { inTransaction } from "../../../infrastructure/transaction";
import type { EmailSender } from "../../../shared/EmailSender";
import { circulationDependencies } from "./dependencies";
import { loanReceiptJob, scheduleLoanReceipt, sendLoanReceipt } from "./loanReceiptJob";
import { brazilianDate } from "./receiptEmail";
import { RegisterLoan } from "../features/register-loan/RegisterLoan";

/*
 * Testes de aceite da atividade. Cada um começa como test.todo: o bun test lista,
 * mas não executa. Quando implementar a parte que um teste cobre, troque
 * test.todo por test e rode a suíte. Não altere o que os testes esperam.
 */

// a suíte roda sem QUEUE_URL; aqui o cliente da fila é explícito, como nos testes da aula
const queue = createRedis(process.env.TEST_QUEUE_URL ?? "redis://fila:6379");
afterAll(() => queue.close());

// sem espera: cada volta do worker lê o que há na fila e devolve o controle ao teste
const workerOptions = { client: queue, consumer: "teste", blockMs: null, retryAfterMs: 300, maxDeliveries: 2 };

/** Um provedor de e-mail fora do ar. */
const failing: EmailSender = {
  async send() {
    throw new Error("provedor de e-mail fora do ar");
  },
};

/** Um empréstimo de um leitor novo, registrado pelo caso de uso, sem passar pela rota: o comprovante ainda não saiu. */
async function newLoan() {
  const reader = await testReader();
  const copy = await testCopy();
  const loan = await inTransaction((tx) => new RegisterLoan(...circulationDependencies(tx)).execute(reader.id, copy.id));
  return { reader, loan };
}

async function receiptSentAt(loanId: number): Promise<Date | null> {
  const [row] = await sql`SELECT comprovante_enviado_em AS "sentAt" FROM emprestimos WHERE id = ${loanId}`;
  return row.sentAt;
}

const sentTo = (sender: MemorySender, address: string) => sender.sent.filter((email) => email.to === address);

test("o stream dos comprovantes pertence à Circulação", () => {
  expect(loanReceiptJob(null).stream).toBe(`${process.env.REDIS_PREFIX ?? "biblioteca"}:circulacao:fila:comprovantes`);
});

test("UC19 API: sem fila, o comprovante sai na hora, para o e-mail do leitor", async () => {
  const reader = await testReader();
  const copy = await testCopy();
  const response = await api("POST", "/emprestimos", { leitorId: reader.id, exemplarId: copy.id });
  expect(response.status).toBe(201);
  const loan = await response.json();
  // a suíte manda os e-mails para a caixa de saída em memória (EMAIL_PROVIDER=memory)
  const received = sentTo(emailSender as MemorySender, reader.email);
  expect(received).toHaveLength(1);
  expect(received[0]).toMatchObject({ toName: reader.nome, subject: `Comprovante de empréstimo: ${loan.livroTitulo}` });
  expect(received[0]!.text).toContain(`exemplar ${loan.numeroRegistro}`);
  expect(received[0]!.text).toContain(`Devolva até ${brazilianDate(loan.dataPrevistaDevolucao)}`);
  expect(await receiptSentAt(loan.id)).not.toBeNull();
});

test("UC19: com fila, a mensagem leva só o id do empréstimo, e o worker manda o comprovante", async () => {
  const { reader, loan } = await newLoan();
  const sender = createMemorySender();
  const job = loanReceiptJob(sender);
  await prepareGroups([job], queue);
  await scheduleLoanReceipt(loan.id, { client: queue, sender });
  // publicou e voltou: ninguém mandou nada ainda
  expect(sender.sent).toHaveLength(0);
  const [last] = await queue.send("XREVRANGE", [job.stream, "+", "-", "COUNT", "1"]) as [string, string[]][];
  const fields = last![1];
  expect(JSON.parse(fields[fields.indexOf("dados") + 1]!)).toEqual({ loanId: loan.id });
  await workOnce([job], workerOptions);
  expect(sentTo(sender, reader.email)).toHaveLength(1);
  expect(await receiptSentAt(loan.id)).not.toBeNull();
});

test("a mesma mensagem entregue duas vezes manda um e-mail só", async () => {
  const { reader, loan } = await newLoan();
  const sender = createMemorySender();
  const job = loanReceiptJob(sender);
  await job.handle({ loanId: loan.id });
  await job.handle({ loanId: loan.id });
  expect(sentTo(sender, reader.email)).toHaveLength(1);
  expect(await sendLoanReceipt(loan.id, sender)).toBe("já enviado");
});

test("o leitor trocou de e-mail antes do envio: o comprovante vai para o endereço atual", async () => {
  const { reader, loan } = await newLoan();
  const sender = createMemorySender();
  const job = loanReceiptJob(sender);
  await prepareGroups([job], queue);
  await scheduleLoanReceipt(loan.id, { client: queue, sender });
  const newEmail = `${crypto.randomUUID()}@exemplo.com`;
  const update = await api("PUT", `/leitores/${reader.id}`, { nome: reader.nome, cpf: reader.cpf, email: newEmail, telefone: reader.telefone });
  expect(update.status).toBe(200);
  await workOnce([job], workerOptions);
  expect(sentTo(sender, reader.email)).toHaveLength(0);
  expect(sentTo(sender, newEmail)).toHaveLength(1);
});

test("se o provedor falha, a tarefa lança e o comprovante não fica marcado; a nova tentativa envia", async () => {
  const { reader, loan } = await newLoan();
  await expect(loanReceiptJob(failing).handle({ loanId: loan.id })).rejects.toThrow("provedor de e-mail fora do ar");
  expect(await receiptSentAt(loan.id)).toBeNull();
  const sender = createMemorySender();
  await loanReceiptJob(sender).handle({ loanId: loan.id });
  expect(sentTo(sender, reader.email)).toHaveLength(1);
  expect(await receiptSentAt(loan.id)).not.toBeNull();
});

test("sem fila, o comprovante sai na hora; se o provedor falha, pedir o comprovante não lança", async () => {
  const { reader, loan } = await newLoan();
  await scheduleLoanReceipt(loan.id, { client: null, sender: failing });
  expect(await receiptSentAt(loan.id)).toBeNull();
  const sender = createMemorySender();
  await scheduleLoanReceipt(loan.id, { client: null, sender });
  expect(sentTo(sender, reader.email)).toHaveLength(1);
});

test("empréstimo inexistente não manda nada nem lança; mensagem sem o id falha", async () => {
  const sender = createMemorySender();
  expect(await sendLoanReceipt(2147483647, sender)).toBe("empréstimo não encontrado");
  await loanReceiptJob(sender).handle({ loanId: 2147483647 });
  await expect(loanReceiptJob(sender).handle({ loanId: "7" } as never)).rejects.toThrow("sem o id do empréstimo");
  await expect(loanReceiptJob(sender).handle(null as never)).rejects.toThrow("sem o id do empréstimo");
  expect(sender.sent).toHaveLength(0);
});

test("sem remetente configurado, nada é enviado nem marcado", async () => {
  const { loan } = await newLoan();
  expect(await sendLoanReceipt(loan.id, null)).toBe("sem remetente");
  await loanReceiptJob(null).handle({ loanId: loan.id });
  expect(await receiptSentAt(loan.id)).toBeNull();
});

/*
 * Discussão (passo 07):
 * 1. Envio antes de marcar: se o processo cair entre os dois, uma nova tentativa
 *    pode repetir o e-mail. Marcar antes pode perder o comprovante se o processo
 *    cair antes do envio. Para um comprovante, prefiro a possível repetição à
 *    perda. Uma cobrança exige uma chave de idempotência aceita pelo provedor.
 * 2. Uma queda entre COMMIT e XADD pode deixar o empréstimo sem tarefa. Uma
 *    varredura pode republicar ids com comprovante_enviado_em nula, respeitando
 *    um marco de implantação para não enviar empréstimos antigos e um intervalo
 *    de segurança para os recém-registrados. O outbox gravaria a tarefa junto
 *    com o empréstimo, e um publicador cuidaria das tentativas de publicação.
 * 3. A consulta da marca evita reenvio após um envio concluído, mas não impede
 *    dois workers simultâneos de lerem a coluna nula. O limite de 5 s do Mailpit
 *    reduz a chance de ultrapassar os 10 s de reentrega; não é garantia absoluta.
 *    Um UPDATE condicional antes do envio pode reservar a tarefa, mas exige
 *    desfazer a marca na falha e recuperar reservas se o processo cair. A garantia
 *    mais forte depende de idempotência no provedor ou coordenação adicional.
 * 4. Após corrigir a causa, republicaria os dados válidos com XADD no stream
 *    original, conferiria o resultado e só então removeria a entrada de mortas.
 *    A marca evita reenviar comprovantes já concluídos; a janela do item 1 ainda
 *    existe. loanId como string precisa ser corrigido antes de republicar.
 *    A operação deve monitorar o lag e a quantidade de mortas, com alertas para
 *    quem mantém o serviço e uma rotina documentada de reprocessamento.
 * 5. O e-mail passa pelo Postgres, pela porta e pela memória do worker, pelo HTTP
 *    do provedor e pelo Mailpit ou provedor real; também pode ficar em backups e
 *    na caixa do destinatário. A fila e suas mortas guardam só o id: isso reduz
 *    cópias do endereço e sua exposição em ferramentas, logs e backups da fila.
 */
