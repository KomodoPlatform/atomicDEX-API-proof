use super::*;
use crate::z_coin::z_htlc::z_send_dex_fee;
use common::block_on;
use common::mm_ctx::MmCtxBuilder;
use common::now_ms;
use std::time::Duration;
use zcash_client_backend::encoding::decode_extended_spending_key;
use zcash_primitives::consensus::BlockHeight;
use zcash_primitives::sapling::keys::OutgoingViewingKey;
use zcash_primitives::sapling::note_encryption::try_sapling_output_recovery;

#[test]
fn zombie_coin_send_and_refund_maker_payment() {
    let conf = json!({
        "coin": "ZOMBIE",
        "asset": "ZOMBIE",
        "fname": "ZOMBIE (TESTCOIN)",
        "txversion": 4i16,
        "overwintered": 1i16,
        "mm2": 1i16,
    });
    let req = json!({
        "method": "enable",
        "coin": "ZOMBIE"
    });

    let ctx = MmCtxBuilder::default().into_mm_arc();
    let priv_key = [1; 32];
    let z_key = decode_extended_spending_key(z_mainnet_constants::HRP_SAPLING_EXTENDED_SPENDING_KEY, "secret-extended-key-main1q0k2ga2cqqqqpq8m8j6yl0say83cagrqp53zqz54w38ezs8ly9ly5ptamqwfpq85u87w0df4k8t2lwyde3n9v0gcr69nu4ryv60t0kfcsvkr8h83skwqex2nf0vr32794fmzk89cpmjptzc22lgu5wfhhp8lgf3f5vn2l3sge0udvxnm95k6dtxj2jwlfyccnum7nz297ecyhmd5ph526pxndww0rqq0qly84l635mec0x4yedf95hzn6kcgq8yxts26k98j9g32kjc8y83fe").unwrap().unwrap();

    let db_dir = PathBuf::from("./for_tests");
    let params = UtxoActivationParams::from_legacy_req(&req).unwrap();
    let coin = block_on(z_coin_from_conf_and_params_with_z_key(
        &ctx, "ZOMBIE", &conf, &params, &priv_key, db_dir, z_key,
    ))
    .unwrap();

    let lock_time = (now_ms() / 1000) as u32 - 3600;
    let taker_pub = coin.utxo_arc.priv_key_policy.key_pair_or_err().unwrap().public();
    let secret_hash = [0; 20];
    let tx = coin
        .send_maker_payment(
            lock_time,
            taker_pub,
            taker_pub,
            &secret_hash,
            "0.01".parse().unwrap(),
            &None,
        )
        .wait()
        .unwrap();
    println!("swap tx {}", hex::encode(&tx.tx_hash().0));

    let refund_tx = coin
        .send_maker_refunds_payment(&tx.tx_hex(), lock_time, &*taker_pub, &secret_hash, &priv_key, &None)
        .wait()
        .unwrap();
    println!("refund tx {}", hex::encode(&refund_tx.tx_hash().0));
}

#[test]
fn zombie_coin_send_and_spend_maker_payment() {
    let conf = json!({
        "coin": "ZOMBIE",
        "asset": "ZOMBIE",
        "fname": "ZOMBIE (TESTCOIN)",
        "txversion": 4i16,
        "overwintered": 1i16,
        "mm2": 1i16,
    });
    let req = json!({
        "method": "enable",
        "coin": "ZOMBIE"
    });

    let ctx = MmCtxBuilder::default().into_mm_arc();
    let priv_key = [1; 32];
    let z_key = decode_extended_spending_key(z_mainnet_constants::HRP_SAPLING_EXTENDED_SPENDING_KEY, "secret-extended-key-main1q0k2ga2cqqqqpq8m8j6yl0say83cagrqp53zqz54w38ezs8ly9ly5ptamqwfpq85u87w0df4k8t2lwyde3n9v0gcr69nu4ryv60t0kfcsvkr8h83skwqex2nf0vr32794fmzk89cpmjptzc22lgu5wfhhp8lgf3f5vn2l3sge0udvxnm95k6dtxj2jwlfyccnum7nz297ecyhmd5ph526pxndww0rqq0qly84l635mec0x4yedf95hzn6kcgq8yxts26k98j9g32kjc8y83fe").unwrap().unwrap();

    let db_dir = PathBuf::from("./for_tests");
    let params = UtxoActivationParams::from_legacy_req(&req).unwrap();
    let coin = block_on(z_coin_from_conf_and_params_with_z_key(
        &ctx, "ZOMBIE", &conf, &params, &priv_key, db_dir, z_key,
    ))
    .unwrap();

    let lock_time = (now_ms() / 1000) as u32 - 1000;
    let taker_pub = coin.utxo_arc.priv_key_policy.key_pair_or_err().unwrap().public();
    let secret = [0; 32];
    let secret_hash = dhash160(&secret);
    let tx = coin
        .send_maker_payment(
            lock_time,
            taker_pub,
            taker_pub,
            &*secret_hash,
            "0.01".parse().unwrap(),
            &None,
        )
        .wait()
        .unwrap();
    println!("swap tx {}", hex::encode(&tx.tx_hash().0));

    let maker_pub = taker_pub;
    let spend_tx = coin
        .send_taker_spends_maker_payment(&tx.tx_hex(), lock_time, &*maker_pub, &secret, &priv_key, &None)
        .wait()
        .unwrap();
    println!("spend tx {}", hex::encode(&spend_tx.tx_hash().0));
}

#[test]
fn zombie_coin_send_dex_fee() {
    let conf = json!({
        "coin": "ZOMBIE",
        "asset": "ZOMBIE",
        "fname": "ZOMBIE (TESTCOIN)",
        "txversion": 4i16,
        "overwintered": 1i16,
        "mm2": 1i16,
    });
    let req = json!({
        "method": "enable",
        "coin": "ZOMBIE"
    });

    let ctx = MmCtxBuilder::default().into_mm_arc();
    let priv_key = [1; 32];
    let z_key = decode_extended_spending_key(z_mainnet_constants::HRP_SAPLING_EXTENDED_SPENDING_KEY, "secret-extended-key-main1q0k2ga2cqqqqpq8m8j6yl0say83cagrqp53zqz54w38ezs8ly9ly5ptamqwfpq85u87w0df4k8t2lwyde3n9v0gcr69nu4ryv60t0kfcsvkr8h83skwqex2nf0vr32794fmzk89cpmjptzc22lgu5wfhhp8lgf3f5vn2l3sge0udvxnm95k6dtxj2jwlfyccnum7nz297ecyhmd5ph526pxndww0rqq0qly84l635mec0x4yedf95hzn6kcgq8yxts26k98j9g32kjc8y83fe").unwrap().unwrap();

    let db_dir = PathBuf::from("./for_tests");
    let params = UtxoActivationParams::from_legacy_req(&req).unwrap();
    let coin = block_on(z_coin_from_conf_and_params_with_z_key(
        &ctx, "ZOMBIE", &conf, &params, &priv_key, db_dir, z_key,
    ))
    .unwrap();

    let tx = block_on(z_send_dex_fee(&coin, "0.01".parse().unwrap(), &[1; 16])).unwrap();
    println!("dex fee tx {}", tx.txid());
}

#[test]
fn prepare_zombie_sapling_cache() {
    let conf = json!({
        "coin": "ZOMBIE",
        "asset": "ZOMBIE",
        "fname": "ZOMBIE",
        "txversion": 4i16,
        "overwintered": 1i16,
        "mm2": 1i16,
    });
    let req = json!({
        "method": "enable",
        "coin": "ZOMBIE"
    });

    let ctx = MmCtxBuilder::default().into_mm_arc();
    let priv_key = [1; 32];
    let z_key = decode_extended_spending_key(z_mainnet_constants::HRP_SAPLING_EXTENDED_SPENDING_KEY, "secret-extended-key-main1q0k2ga2cqqqqpq8m8j6yl0say83cagrqp53zqz54w38ezs8ly9ly5ptamqwfpq85u87w0df4k8t2lwyde3n9v0gcr69nu4ryv60t0kfcsvkr8h83skwqex2nf0vr32794fmzk89cpmjptzc22lgu5wfhhp8lgf3f5vn2l3sge0udvxnm95k6dtxj2jwlfyccnum7nz297ecyhmd5ph526pxndww0rqq0qly84l635mec0x4yedf95hzn6kcgq8yxts26k98j9g32kjc8y83fe").unwrap().unwrap();

    let db_dir = PathBuf::from("./for_tests");
    let params = UtxoActivationParams::from_legacy_req(&req).unwrap();
    let coin = block_on(z_coin_from_conf_and_params_with_z_key(
        &ctx, "ZOMBIE", &conf, &params, &priv_key, db_dir, z_key,
    ))
    .unwrap();

    while !coin.z_fields.sapling_state_synced.load(AtomicOrdering::Relaxed) {
        std::thread::sleep(Duration::from_secs(1));
    }
}

#[test]
fn zombie_coin_validate_dex_fee() {
    let conf = json!({
        "coin": "ZOMBIE",
        "asset": "ZOMBIE",
        "fname": "ZOMBIE (TESTCOIN)",
        "txversion": 4i16,
        "overwintered": 1i16,
        "mm2": 1i16,
    });
    let req = json!({
        "method": "enable",
        "coin": "ZOMBIE"
    });

    let ctx = MmCtxBuilder::default().into_mm_arc();
    let priv_key = [1; 32];
    let z_key = decode_extended_spending_key(z_mainnet_constants::HRP_SAPLING_EXTENDED_SPENDING_KEY, "secret-extended-key-main1q0k2ga2cqqqqpq8m8j6yl0say83cagrqp53zqz54w38ezs8ly9ly5ptamqwfpq85u87w0df4k8t2lwyde3n9v0gcr69nu4ryv60t0kfcsvkr8h83skwqex2nf0vr32794fmzk89cpmjptzc22lgu5wfhhp8lgf3f5vn2l3sge0udvxnm95k6dtxj2jwlfyccnum7nz297ecyhmd5ph526pxndww0rqq0qly84l635mec0x4yedf95hzn6kcgq8yxts26k98j9g32kjc8y83fe").unwrap().unwrap();

    let db_dir = PathBuf::from("./for_tests");
    let params = UtxoActivationParams::from_legacy_req(&req).unwrap();
    let coin = block_on(z_coin_from_conf_and_params_with_z_key(
        &ctx, "ZOMBIE", &conf, &params, &priv_key, db_dir, z_key,
    ))
    .unwrap();

    // https://zombie.explorer.lordofthechains.com/tx/ec620194c33eba004904f34c93f4f005a7544988771af1c5a527f65c08e4a4aa
    let tx_hex = "0400008085202f89000000000000af330000e803000000000000015c3fc69c0eb25dc2b75593464af5b937da35816a2ffeb9b79f3da865c2187083a0b143011810109ab0ed410896aff77bcfbc8a8f5b9bfe0d273716095cfe401cbd97c66a999384aa12a571abc39508b113de0ad0816630fea67f18d68572c52be4364f812f9796e1084ee6c28d1419dac4767d12a7a33662536c2c1ffa7e221d843c9f2bf2601f34cc71a1e1c42041fab87e617ae00b796aa070280060e9cdc30e69e80367e6105e792bbefcd93f00c48ce8278c4eb36c8846cb94d5adcb273ce91decf79196461f7969d6a7031878c6c8e81edd4532a5c57bbaeeea4ed5f4440cef90f19020079c69e05325e63350e9cb9eac44a3d4937111a3c6dc00c79d4dfe72c1e73a6e00ad0aa1aded83f0b778ab92319fcdae19c2946c50c370d243fe6dfa4f92803dcec1992af0d91f0cda8ccbee2a5321f708fc0156d29b51a015b3fb70f543c7713b8547d24e6916caefca17edf1f4109099177498cb30f9305b5169ab1f2e3e4a83e789b5687f3f5f5013d917e2e6babc8ca4507cb349d1e5a30602f557bcbd6574c7fcb5779ce286bdd10fe5db58abadcacf5eaa9e5d3575e30e439d0c62494bc045456e7b6b03f5304a8ff8878f01883f8c473e066f8159bdc111a03d96670f4b29acd919d8b9674897e056c7ac6ef4da155ce7d923f2bedcd51f2198c2be360e03ef2373df94d1e63ba507effc2f9b2f1ccfed09f2f26b8c619415d4a90f556e4b9350099f58fb10a33986945a1512879fdae66e9ef94671764ecdc558ed2d760f7bd3ce2dedfdb4fc7e3aa26903288e16f34214632d8727f68d47389ff687f681b3b285896d3214b9eb60271d87f3223f20e4ddf39513c07fe3420eefa9e7372fff51c83468161d9ffe745533b02917e4ccf87a213c884042938511bb7ccbe6b54392897b1ba111d127ec2c16ba167bb5a65d7819295ceedc5b8faf493c71ed722b72578c62be7d59449bd218196e1f43c3a8bb4875c3bcce1adcb6c4afa6398a7276583c60dbe609c9819bf66385e6cff4b27090aa1dccd0a2f86ca3b3871f2077db44c17d57bba98f9809e6000676600ad70560cbf285354f979d24a5de6e8b0c65ee1a89e28f58f430d20988cae8b0a9690cf79519efc227d54ca739ce3dcde73ac6e624c00b120d6955b40b854b00b1b53dc18cc35cd4792716f3e0bc6552bf0ba4616d1b22900cebede31fbe4b722de1f11c0577abe2ca0614c9d6f24cb56e2b4c840b8573c503ca1d4bf9e671a583b04dd51af10cfc709e89965c5150d7fb6b8c924812e6c9d31025d30e8367defb39e71fda095a16c0e1a70b528799d8c4852b3adb700b113bf5de1d6ec6c7742a1ef678228930ec767e406b36a55fe4a8108236cf0487901e35b50312facad257fd9ba2be154fbc674b33240fffaffc149f26238c5b188107df049cc615289ab8ee6f12a868379f6e362b059ba7c3dde3f02a91a08316c194ad7e556d390d38e6442212502f84cb22fc7dbab262984d2155ebeee3e4109033e57e761e9ab701512cf2635fe92f12d42953ce33f020ad4606125477318f88f673517831f43e548c5ef1d6d4aef7d850fdc0d35bc38a69ac02ccc7436eb711c6303cd306b34931bf1a4cbaed6940ede588e2abf7835718e4afed606d71cdb48146598db31d024347ba9eb289f714bfa7a3670392b3a5e35b6359f6626ed07cca451f0389e4423bb531baf409c48279df489d0073ccf17676eb5c5caa732b104894c2bcf311774f1f8c0b8b6fa313437bc1209f29ee64ccb40a07bb0cf928c77ca6b6a4fe287b1dc6df678a32b8dda35876211d5f929f90a6cc772bd171d15f50da9de8f11a241be98d205b2c53a78a5ba1bce0e782ee88512c3fc815fe843c6b5ffae1b80f1bdd5132b84a813e5157d3096034011fec2f0543f9c30a119d87e8b66e9a857d833d45fe55352871f68aaf8757c03f3b82f1cbd13c56d1843b9d2ebf7fe42f41ab0493dc9491813456fd1e0466bfdfb87a684cba8944df2fd8d3703617383137613a853a3725b366079c3760bbce60f2a88fa2cc579a6ddc9813185cb26873e6c09e43b6db73e4a44d30eebdae38bdaae9f6f1c38941b342ba67822b039f35878e54aadc4b1861df8803494f739d07b0d8b7815d1b55932bcdda80f612f97e0a0c288a7daf3aee1eb0db33fa030082b439a6d0c8d1043a718747acc398913f89e09cb0c95be96fdc9b8aa01f8eba0bd543528035fb7442ce9c6fa5e5539d4dfe29f2a9400d2d122d61037b9df584c5738b851a0d8f6bb6cf553efbdeefc3db3718681a75cb90398fa54c8dd1e696de8dba5ec977c4e2909f4977fde39847f2c0d8f9f9927e9a6cc9466b90d7745e678baa32100cb1ca7d2969c6ec9f35b222f3f4126a7965c40e5da75f183f73d33d325f25a371f5767c6b5bca141c30ed409ffce5f8e073bfb0a85512d0594c96b80cd5d7b73ac3dca494aa9dc7085ad594b46eb28fd1df84afc8a71dd63bc5d23eaf21238706a205d643bd238fe01b32dcd50c93047498ed54bb01cf2108d326f7e3c0538a9e6cc79090ccee6cf47e7fd3cc5cf41aad6905c5d099cea22effdcd4bb7b8d85ba3e3d703c34863d2540936976c774e5c4cb020873873a186c3bab67b1a47c4029f2880cbadd1cd7d82a6c649b073aa0c938b5f28e9173a64c72c81745bc8df6706bf6e320b5e96820970322f21d633a2c28b23d79b8edbc8a13eafa2a5241d7bb59b341779fe6f5db2994567caefaec23b7b7c55a73dbc6614bb958bc1d62838c56197a3eceefeb1dc4f505645548f2dd8848e4046aca421548235f1945725f82f03b0ba5c774ddea6f9524cdcc302ee4712ef7d4bc1c16d7aa578d8fd8ceb680c16fcc6ca6a40afdcef6f89e81bd92f7d1f6e39c9c57f3239a1fcb23d649f8757348214572e53bc2c2c7ff8bce6d48df6e3c53ab7014a55c9296d05998a0d1b53749d9561541eb0cf6e1bfa65141ce9b6c30fe4f68cd8e869feba82675ec43bf953ab2994533d6d1af1705130243d9b9ee4088b635d6b4db5603b8784f4fe77d4b0d8a7935c06198d12fa0fc6e1ad2ddef96e7f9ab6103a2a29739ca3af9fe1736cdf49162e77d6f17d063f04dc2e1358af3da993fb3824e59575a9f15c7c429efd059477429be0c2a5b126078a8f8b1088d35aae59eac0897dfa4d45179947bad401c7417df2fac46f8782a2069f83cc18eda4d0070167878ad72f5d255e300a6368e0d390d3d0206aba68772b1e9d73c97406a0a5d80b7b8360502a9e7cb471fb5bd49ce9eee3a16f82aadca47327ccaa00a0575ed7191ffb710dd1ab7f801";
    let tx_bytes = hex::decode(tx_hex).unwrap();
    let tx = ZTransaction::read(tx_bytes.as_slice()).unwrap();
    let tx = tx.into();

    // Invalid amount should return an error
    let err = coin
        .validate_fee(&tx, &[], &[], &"0.001".parse().unwrap(), 12000, &[1; 16])
        .wait()
        .unwrap_err();
    println!("{}", err);
    assert!(err.contains("Dex fee has invalid amount"));

    // Invalid memo should return an error
    let err = coin
        .validate_fee(&tx, &[], &[], &"0.01".parse().unwrap(), 12000, &[2; 16])
        .wait()
        .unwrap_err();
    println!("{}", err);
    assert!(err.contains("Dex fee has invalid memo"));

    // Confirmed before min block
    let err = coin
        .validate_fee(&tx, &[], &[], &"0.01".parse().unwrap(), 14000, &[1; 16])
        .wait()
        .unwrap_err();
    println!("{}", err);
    assert!(err.contains("confirmed before min block"));

    // Success validation
    coin.validate_fee(&tx, &[], &[], &"0.01".parse().unwrap(), 12000, &[1; 16])
        .wait()
        .unwrap();
}

struct ZombieTransactionInfo {
    shielded_index: u32,
    amount: u32,
    tx_hash: String,
}

impl TryFrom<&Row<'_>> for ZombieTransactionInfo {
    type Error = SqliteError;

    fn try_from(row: &Row<'_>) -> Result<ZombieTransactionInfo, SqliteError> {
        let index = row.get(0)?;
        let amount = row.get(1)?;
        let hash = row.get(2)?;

        Ok(ZombieTransactionInfo {
            shielded_index: index,
            amount,
            tx_hash: hash,
        })
    }
}

fn init_db(sql: &Connection) -> Result<(), SqliteError> {
    const INIT_ZOMBIE_TABLES_STMT: &str = "
        BEGIN;
        CREATE TABLE IF NOT EXISTS zombie_height (height INTEGER NOT NULL PRIMARY KEY);
        CREATE TABLE IF NOT EXISTS zombie_tx_info (
        id INTEGER NOT NULL PRIMARY KEY,
        amount INTEGER NOT NULL,
        tx_hash VARCHAR(255) NOT NULL);
        COMMIT;";

    sql.execute_batch(INIT_ZOMBIE_TABLES_STMT).map(|_| ())
}

fn query_processed_height(conn: &Connection) -> Result<u32, SqliteError> {
    const QUERY_BLOCK_STMT: &str = "SELECT COUNT(height) FROM zombie_height";

    conn.query_row(QUERY_BLOCK_STMT, NO_PARAMS, |row: &Row<'_>| Ok(row.get(0)?))
}

fn insert_block_height(conn: &Connection, height: u32) -> Result<(), SqliteError> {
    const INSERT_HEIGHT_STMT: &str = "INSERT INTO zombie_height (height) VALUES (?1);";
    let h = height.to_sql()?;
    conn.execute(INSERT_HEIGHT_STMT, &[h]).map(|_| ())
}

fn query_count_tbl_zombie_tx_info(conn: &Connection) -> Result<u32, SqliteError> {
    const QUERY_COUNT_STMT: &str = "SELECT COUNT(id) FROM zombie_tx_info";

    conn.query_row(QUERY_COUNT_STMT, NO_PARAMS, |row: &Row<'_>| Ok(row.get(0)?))
}

fn insert_tx_info(conn: &Connection, info: ZombieTransactionInfo) -> Result<(), SqliteError> {
    const INSERT_TX_INFO_STMT: &str = "INSERT INTO zombie_tx_info (id, amount, tx_hash) VALUES (?1, ?2, ?3);";
    let index = info.shielded_index.to_sql()?;
    let amount = info.amount.to_sql()?;
    let tx_hash = info.tx_hash.to_sql()?;

    conn.execute(INSERT_TX_INFO_STMT, &[index, amount, tx_hash]).map(|_| ())
}

fn query_latest_shielded_index(conn: &Connection) -> Result<u32, SqliteError> {
    const QUERY_LATEST_SHIELDED_INDEX_STMT: &str = "SELECT id FROM zombie_tx_info ORDER BY id desc LIMIT 1";

    conn.query_row(QUERY_LATEST_SHIELDED_INDEX_STMT, NO_PARAMS, |row: &Row<'_>| {
        Ok(row.get(0)?)
    })
}

const DEX_FEE_OVK: OutgoingViewingKey = OutgoingViewingKey([7; 32]);
async fn scan_zombie_recoverable_transactions_impl() {
    let conf = json!({
        "coin": "ZOMBIE",
        "asset": "ZOMBIE",
        "fname": "ZOMBIE (TESTCOIN)",
        "txversion": 4i16,
        "overwintered": 1i16,
        "mm2": 1i16,
    });

    let req = json!({
        "method": "enable",
        "coin": "ZOMBIE"
    });

    let ctx = MmCtxBuilder::default().into_mm_arc();
    let priv_key = [1; 32];
    let z_key = decode_extended_spending_key(z_mainnet_constants::HRP_SAPLING_EXTENDED_SPENDING_KEY, "secret-extended-key-main1q0k2ga2cqqqqpq8m8j6yl0say83cagrqp53zqz54w38ezs8ly9ly5ptamqwfpq85u87w0df4k8t2lwyde3n9v0gcr69nu4ryv60t0kfcsvkr8h83skwqex2nf0vr32794fmzk89cpmjptzc22lgu5wfhhp8lgf3f5vn2l3sge0udvxnm95k6dtxj2jwlfyccnum7nz297ecyhmd5ph526pxndww0rqq0qly84l635mec0x4yedf95hzn6kcgq8yxts26k98j9g32kjc8y83fe").unwrap().unwrap();

    let db_dir = PathBuf::from("./for_tests");
    let params = UtxoActivationParams::from_legacy_req(&req).unwrap();
    let coin = z_coin_from_conf_and_params_with_z_key(&ctx, "ZOMBIE", &conf, &params, &priv_key, db_dir, z_key)
        .await
        .unwrap();

    let native_client = match coin.rpc_client() {
        UtxoRpcClientEnum::Native(n) => n,
        _ => unimplemented!("Implemented only for native client"),
    };
    init_db(&coin.sqlite_conn()).expect("Creation should not fail");
    let current_block = match coin.rpc_client().get_block_count().compat().await {
        Ok(b) => b,
        Err(e) => {
            panic!("Error {} on getting block count", e);
        },
    };

    let mut processed_height = query_processed_height(&coin.sqlite_conn()).expect("Count should not fail") as u64 + 1;
    let count = query_count_tbl_zombie_tx_info(&coin.sqlite_conn()).expect("Count should not fail");
    let mut shielded_index = if count > 0 {
        query_latest_shielded_index(&coin.sqlite_conn()).expect("Count shuld not fail") as u64 + 1
    } else {
        0
    };
    while processed_height <= current_block {
        let block = match native_client.get_block_by_height(processed_height).await {
            Ok(b) => b,
            Err(e) => {
                log::error!("Error {} on getting block", e);
                Timer::sleep(1.).await;
                continue;
            },
        };
        for hash in block.tx {
            let tx_bytes = native_client
                .get_transaction_bytes(&hash)
                .compat()
                .await
                .expect("Panic here to avoid storing invalid tree state to the DB");
            let tx = ZTransaction::read(tx_bytes.as_slice()).unwrap();
            for shielded_out in tx.shielded_outputs.iter() {
                shielded_index += 1;
                if let Some((note, _, _)) = try_sapling_output_recovery(
                    &ARRRConsensusParams {},
                    BlockHeight::from_u32(processed_height as u32),
                    &DEX_FEE_OVK,
                    shielded_out,
                ) {
                    let info = ZombieTransactionInfo {
                        shielded_index: shielded_index as u32,
                        amount: note.value as u32,
                        tx_hash: format!("{:02x}", hash),
                    };
                    insert_tx_info(&coin.sqlite_conn(), info).expect("Insertion should not fail");
                }
            }
        }
        insert_block_height(&coin.sqlite_conn(), processed_height as u32).expect("Insertion should not fail");
        processed_height += 1;
    }
}

#[test]
#[cfg(not(target_arch = "wasm32"))]
fn scan_zombie_recoverable_transactions() { block_on(scan_zombie_recoverable_transactions_impl()); }
