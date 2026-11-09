import init, { serve_ledger, startup_failed } from './pkg/worker_ledger.js';

const options = new URL(self.location.href).searchParams;
await init();
if (options.has('fail')) {
  await startup_failed('requested initialization failure');
} else {
  try {
    await serve_ledger(
      options.get('name') ?? 'ledger.sqlite3',
      options.has('lose_reply'),
      options.has('fail_cleanup'),
    );
  } catch (error) {
    // Surface runtime traps and failed replies to the caller's Worker error listener.
    setTimeout(() => { throw error; });
  }
}
