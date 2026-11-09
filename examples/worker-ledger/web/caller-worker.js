import init, { LedgerClient } from './pkg/worker_ledger.js';

await init();
let database;
self.onmessage = async ({ data: { id, method, argument } }) => {
  try {
    let result;
    switch (method) {
      case 'open':
        database = await LedgerClient.open(argument);
        break;
      case 'exercise': result = await database.exercise(); break;
      case 'balance': result = await database.balance(); break;
      case 'debit': result = await database.debit(BigInt(argument)); break;
      case 'cancel': result = database.cancel_debit(BigInt(argument)); break;
      case 'long_query': result = await database.long_query(); break;
      case 'close': result = await database.close(); break;
      case 'terminate': result = database.terminate(); break;
      case 'ping': result = 'caller-responsive'; break;
      default: throw new Error(`Unknown method: ${method}`);
    }
    self.postMessage({ id, result: typeof result === 'bigint' ? result.toString() : result });
  } catch (error) {
    self.postMessage({ id, error: String(error) });
  }
};
self.postMessage({ ready: true });
