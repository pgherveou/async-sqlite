use super::*;
use async_sqlite::{ClientBuilder, NativeDatabase};

#[test]
fn shared_ledger_contract_on_native() {
    smol::block_on(async {
        let client = ClientBuilder::new().open().await.unwrap();
        client.conn(initialize).await.unwrap();
        let database = NativeDatabase::new(client, handle);
        exercise(&database).await.unwrap();
        database.close().await.unwrap();
    });
}
