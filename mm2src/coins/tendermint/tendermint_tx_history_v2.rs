use super::iris::htlc_proto::QueryHtlcResponseProto;
use super::{rpc::*, type_urls::*, AllBalancesResult, TendermintCoin, TendermintCoinRpcError, TendermintToken};

use crate::my_tx_history_v2::{CoinWithTxHistoryV2, MyTxHistoryErrorV2, MyTxHistoryTarget, TxHistoryStorage};
use crate::tendermint::iris::htlc::{HTLC_STATE_COMPLETED, HTLC_STATE_OPEN, HTLC_STATE_REFUNDED};
use crate::tendermint::iris::htlc_proto::{ClaimHtlcProtoRep, CreateHtlcProtoRep};
use crate::tendermint::TendermintFeeDetails;
use crate::tx_history_storage::{GetTxHistoryFilters, WalletId};
use crate::utxo::utxo_common::big_decimal_from_sat_unsigned;
use crate::{HistorySyncState, MarketCoinOps, TransactionDetails, TxFeeDetails};
use async_trait::async_trait;
use common::executor::Timer;
use common::log;
use common::state_machine::prelude::*;
use cosmrs::proto::cosmos::bank::v1beta1::MsgSend;
use cosmrs::proto::cosmos::base::v1beta1::Coin as CoinAmount;
use cosmrs::tx::Fee;
use mm2_err_handle::prelude::{MmError, MmResult};
use mm2_metrics::MetricsArc;
use mm2_number::BigDecimal;
use primitives::hash::H256;
use prost::Message;
use rpc::v1::types::Bytes as BytesJson;
use std::collections::{hash_map::Entry, HashMap};

macro_rules! try_or_return_stopped_as_err {
    ($exp:expr, $reason: expr, $fmt:literal) => {
        match $exp {
            Ok(t) => t,
            Err(e) => {
                return Err(Stopped {
                    phantom: Default::default(),
                    stop_reason: $reason(format!("{}: {}", $fmt, e)),
                })
            },
        }
    };
}

#[async_trait]
pub trait TendermintTxHistoryOps: CoinWithTxHistoryV2 + MarketCoinOps + Send + Sync + 'static {
    async fn get_rpc_client(&self) -> MmResult<HttpClient, TendermintCoinRpcError>;

    async fn query_htlc(&self, id: String) -> MmResult<QueryHtlcResponseProto, TendermintCoinRpcError>;

    fn decimals(&self) -> u8;

    fn set_history_sync_state(&self, new_state: HistorySyncState);

    async fn all_balances(&self) -> MmResult<AllBalancesResult, TendermintCoinRpcError>;

    fn all_wallet_ids(&self) -> Vec<WalletId>;

    // Returns Hashmap where key is denom and value is ticker
    fn get_denom_and_ticker_map(&self) -> HashMap<String, String>;
}

#[async_trait]
impl CoinWithTxHistoryV2 for TendermintCoin {
    fn history_wallet_id(&self) -> WalletId { WalletId::new(self.ticker().replace('-', "_")) }

    async fn get_tx_history_filters(
        &self,
        _target: MyTxHistoryTarget,
    ) -> MmResult<GetTxHistoryFilters, MyTxHistoryErrorV2> {
        Ok(GetTxHistoryFilters::for_address(self.account_id.to_string()))
    }
}

#[async_trait]
impl CoinWithTxHistoryV2 for TendermintToken {
    fn history_wallet_id(&self) -> WalletId { WalletId::new(self.ticker().replace('-', "_")) }

    async fn get_tx_history_filters(
        &self,
        _target: MyTxHistoryTarget,
    ) -> MmResult<GetTxHistoryFilters, MyTxHistoryErrorV2> {
        Ok(GetTxHistoryFilters::for_address(
            self.platform_coin.account_id.to_string(),
        ))
    }
}

struct TendermintTxHistoryCtx<Coin: TendermintTxHistoryOps, Storage: TxHistoryStorage> {
    coin: Coin,
    storage: Storage,
    #[allow(dead_code)]
    metrics: MetricsArc,
    balances: AllBalancesResult,
}

struct TendermintInit<Coin, Storage> {
    phantom: std::marker::PhantomData<(Coin, Storage)>,
}

impl<Coin, Storage> TendermintInit<Coin, Storage> {
    fn new() -> Self {
        TendermintInit {
            phantom: Default::default(),
        }
    }
}

#[derive(Debug)]
enum StopReason {
    StorageError(String),
    UnknownError(String),
    RpcClient(String),
    Marshaling(String),
}

struct Stopped<Coin, Storage> {
    phantom: std::marker::PhantomData<(Coin, Storage)>,
    stop_reason: StopReason,
}

impl<Coin, Storage> Stopped<Coin, Storage> {
    fn storage_error<E>(e: E) -> Self
    where
        E: std::fmt::Debug,
    {
        Stopped {
            phantom: Default::default(),
            stop_reason: StopReason::StorageError(format!("{:?}", e)),
        }
    }

    fn unknown(e: String) -> Self {
        Stopped {
            phantom: Default::default(),
            stop_reason: StopReason::UnknownError(e),
        }
    }
}

struct WaitForHistoryUpdateTrigger<Coin, Storage> {
    phantom: std::marker::PhantomData<(Coin, Storage)>,
}

impl<Coin, Storage> WaitForHistoryUpdateTrigger<Coin, Storage> {
    fn new() -> Self {
        WaitForHistoryUpdateTrigger {
            phantom: Default::default(),
        }
    }
}

struct FetchingTransactionsData<Coin, Storage> {
    /// The list of addresses for those we have requested [`UpdatingUnconfirmedTxes::all_tx_ids_with_height`] TX hashses
    /// at the `FetchingTxHashes` state.
    address: String,
    /// Denom - Ticker
    active_assets: HashMap<String, String>,
    phantom: std::marker::PhantomData<(Coin, Storage)>,
}

impl<Coin, Storage> FetchingTransactionsData<Coin, Storage> {
    fn new(address: String, active_assets: HashMap<String, String>) -> Self {
        FetchingTransactionsData {
            address,
            active_assets,
            phantom: Default::default(),
        }
    }
}

impl<Coin, Storage> TransitionFrom<TendermintInit<Coin, Storage>> for Stopped<Coin, Storage> {}
impl<Coin, Storage> TransitionFrom<TendermintInit<Coin, Storage>> for FetchingTransactionsData<Coin, Storage> {}
impl<Coin, Storage> TransitionFrom<WaitForHistoryUpdateTrigger<Coin, Storage>> for Stopped<Coin, Storage> {}
impl<Coin, Storage> TransitionFrom<FetchingTransactionsData<Coin, Storage>> for Stopped<Coin, Storage> {}

impl<Coin, Storage> TransitionFrom<WaitForHistoryUpdateTrigger<Coin, Storage>>
    for FetchingTransactionsData<Coin, Storage>
{
}

impl<Coin, Storage> TransitionFrom<FetchingTransactionsData<Coin, Storage>>
    for WaitForHistoryUpdateTrigger<Coin, Storage>
{
}

#[async_trait]
impl<Coin, Storage> State for WaitForHistoryUpdateTrigger<Coin, Storage>
where
    Coin: TendermintTxHistoryOps,
    Storage: TxHistoryStorage,
{
    type Ctx = TendermintTxHistoryCtx<Coin, Storage>;
    type Result = ();

    async fn on_changed(self: Box<Self>, ctx: &mut Self::Ctx) -> StateResult<Self::Ctx, Self::Result> {
        loop {
            Timer::sleep(30.).await;

            let ctx_balances = ctx.balances.clone();

            let balances = match ctx.coin.all_balances().await {
                Ok(balances) => balances,
                Err(e) => return Self::change_state(Stopped::unknown(e.to_string())),
            };

            if balances != ctx_balances {
                if let Err(e) = ensure_storage_init(&ctx.coin.all_wallet_ids(), &ctx.storage).await {
                    return Self::change_state(Stopped::storage_error(e));
                }
                // Update balances
                ctx.balances = balances;

                return Self::change_state(FetchingTransactionsData::new(
                    ctx.coin.my_address().expect("my_address can't fail"),
                    ctx.coin.get_denom_and_ticker_map(),
                ));
            }
        }
    }
}

#[async_trait]
impl<Coin, Storage> State for FetchingTransactionsData<Coin, Storage>
where
    Coin: TendermintTxHistoryOps,
    Storage: TxHistoryStorage,
{
    type Ctx = TendermintTxHistoryCtx<Coin, Storage>;
    type Result = ();

    async fn on_changed(self: Box<Self>, ctx: &mut Self::Ctx) -> StateResult<Self::Ctx, Self::Result> {
        const TX_PAGE_SIZE: u8 = 50;

        struct TxAmounts {
            total: BigDecimal,
            spent_by_me: BigDecimal,
            received_by_me: BigDecimal,
            my_balance_change: BigDecimal,
        }

        fn get_tx_amounts(coin_amount: &CoinAmount, decimals: u8, spent_by_me: bool) -> Result<TxAmounts, String> {
            let amount: u64 = coin_amount.amount.parse().map_err(|e| format!("{:?}", e))?;
            let amount = big_decimal_from_sat_unsigned(amount, decimals);

            let (spent_by_me, received_by_me) = if spent_by_me {
                (amount.clone(), BigDecimal::default())
            } else {
                (BigDecimal::default(), amount.clone())
            };

            Ok(TxAmounts {
                total: amount,
                my_balance_change: received_by_me.clone() - spent_by_me.clone(),
                spent_by_me,
                received_by_me,
            })
        }

        fn get_fee_details<Coin>(fee: Fee, coin: &Coin) -> Result<TxFeeDetails, String>
        where
            Coin: TendermintTxHistoryOps,
        {
            let fee_coin = fee
                .amount
                .first()
                .ok_or_else(|| "fee coin can't be empty".to_string())?;
            let fee_amount: u64 = fee_coin.amount.to_string().parse().map_err(|e| format!("{:?}", e))?;

            Ok(TxFeeDetails::Tendermint(TendermintFeeDetails {
                coin: coin.platform_ticker().to_string(),
                amount: big_decimal_from_sat_unsigned(fee_amount, coin.decimals()),
                gas_limit: fee.gas_limit.value(),
            }))
        }

        fn upsert_tx_details(
            details_map: &mut HashMap<String, Vec<TransactionDetails>>,
            new_details: TransactionDetails,
        ) {
            match details_map.entry(new_details.coin.clone()) {
                Entry::Vacant(e) => {
                    e.insert(vec![new_details]);
                },
                Entry::Occupied(mut e) => {
                    e.get_mut().push(new_details);
                },
            };
        }

        struct ParseTxDetailsArgs<Coin, Storage, 'a> {
            coin: &'a Coin,
            storage: &'a Storage,
            active_assets: &'a HashMap<String, String>,
            internal_id: BytesJson,
            block_height: u64,
            tx_hash: String,
            tx_body_msg: &'a [u8],
            fee_details: TxFeeDetails,
        }

        async fn parse_send_tx_details<Coin, Storage>(
            args: ParseTxDetailsArgs<Coin, Storage, '_>,
        ) -> Result<Option<TransactionDetails>, StopReason>
        where
            Coin: TendermintTxHistoryOps,
            Storage: TxHistoryStorage,
        {
            let sent_tx = MsgSend::decode(args.tx_body_msg).map_err(|e| StopReason::Marshaling(e.to_string()))?;
            let coin_amount = sent_tx
                .amount
                .first()
                .ok_or_else(|| StopReason::Marshaling("amount can't be empty".to_string()))?;
            let denom = coin_amount.denom.to_lowercase();

            let tx_coin_ticker = match args.active_assets.get(&denom) {
                Some(t) => t,
                None => return Ok(None),
            };

            let wallet_id = WalletId::new(tx_coin_ticker.replace('-', "_"));
            if matches!(
                args.storage.get_tx_from_history(&wallet_id, &args.internal_id).await,
                Ok(Some(_))
            ) {
                log::debug!("Tx '{}' already exists in tx_history. Skipping it.", &args.tx_hash);
                return Ok(None);
            } else {
                log::debug!("Adding tx: '{}' to tx_history.", &args.tx_hash);
            }

            let address = args.coin.my_address().expect("my_address can't fail");
            let tx_amounts = get_tx_amounts(coin_amount, args.coin.decimals(), sent_tx.from_address == address)
                .map_err(StopReason::Marshaling)?;

            Ok(Some(TransactionDetails {
                from: vec![sent_tx.from_address],
                to: vec![sent_tx.to_address.clone()],
                total_amount: tx_amounts.total,
                spent_by_me: tx_amounts.spent_by_me,
                received_by_me: tx_amounts.received_by_me,
                my_balance_change: tx_amounts.my_balance_change,
                tx_hash: args.tx_hash,
                tx_hex: args.tx_body_msg.into(),
                fee_details: Some(args.fee_details),
                block_height: args.block_height,
                coin: tx_coin_ticker.to_string(),
                internal_id: args.internal_id,
                timestamp: common::now_ms() / 1000,
                kmd_rewards: None,
                transaction_type: Default::default(),
            }))
        }

        async fn parse_claim_htlc_tx_details<Coin, Storage>(
            args: ParseTxDetailsArgs<Coin, Storage, '_>,
        ) -> Result<Option<TransactionDetails>, StopReason>
        where
            Coin: TendermintTxHistoryOps,
            Storage: TxHistoryStorage,
        {
            let htlc_tx =
                ClaimHtlcProtoRep::decode(args.tx_body_msg).map_err(|e| StopReason::Marshaling(e.to_string()))?;

            let htlc_response = args
                .coin
                .query_htlc(htlc_tx.id)
                .await
                .map_err(|e| StopReason::RpcClient(e.to_string()))?;

            let htlc_data = match htlc_response.htlc {
                Some(htlc) => htlc,
                None => return Ok(None),
            };

            match htlc_data.state {
                HTLC_STATE_OPEN | HTLC_STATE_COMPLETED | HTLC_STATE_REFUNDED => {},
                _unexpected_state => return Ok(None),
            };

            let coin_amount = htlc_data
                .amount
                .first()
                .ok_or_else(|| StopReason::Marshaling("amount can't be empty".to_string()))?;

            let denom = coin_amount.denom.to_lowercase();
            let tx_coin_ticker = match args.active_assets.get(&denom) {
                Some(t) => t,
                None => return Ok(None),
            };

            let wallet_id = WalletId::new(tx_coin_ticker.replace('-', "_"));
            if matches!(
                args.storage.get_tx_from_history(&wallet_id, &args.internal_id).await,
                Ok(Some(_))
            ) {
                log::debug!("Tx '{}' already exists in tx_history. Skipping it.", &args.tx_hash);
                return Ok(None);
            } else {
                log::debug!("Adding tx: '{}' to tx_history.", &args.tx_hash);
            }

            let address = args.coin.my_address().expect("my_address can't fail");
            let tx_amounts = get_tx_amounts(coin_amount, args.coin.decimals(), htlc_data.sender == address)
                .map_err(StopReason::Marshaling)?;

            Ok(Some(TransactionDetails {
                from: vec![htlc_data.sender],
                to: vec![htlc_data.to.clone()],
                total_amount: tx_amounts.total,
                spent_by_me: tx_amounts.spent_by_me,
                received_by_me: tx_amounts.received_by_me,
                my_balance_change: tx_amounts.my_balance_change,
                tx_hash: args.tx_hash,
                tx_hex: args.tx_body_msg.into(),
                fee_details: Some(args.fee_details),
                block_height: args.block_height,
                coin: tx_coin_ticker.to_string(),
                internal_id: args.internal_id,
                timestamp: common::now_ms() / 1000,
                kmd_rewards: None,
                transaction_type: Default::default(),
            }))
        }

        async fn parse_create_htlc_tx_details<Coin, Storage>(
            args: ParseTxDetailsArgs<Coin, Storage, '_>,
        ) -> Result<Option<TransactionDetails>, StopReason>
        where
            Coin: TendermintTxHistoryOps,
            Storage: TxHistoryStorage,
        {
            let htlc_tx =
                CreateHtlcProtoRep::decode(args.tx_body_msg).map_err(|e| StopReason::Marshaling(e.to_string()))?;

            let coin_amount = htlc_tx
                .amount
                .first()
                .ok_or_else(|| StopReason::Marshaling("amount can't be empty".to_string()))?;

            let denom = coin_amount.denom.to_lowercase();
            let tx_coin_ticker = match args.active_assets.get(&denom) {
                Some(t) => t,
                None => return Ok(None),
            };

            let wallet_id = WalletId::new(tx_coin_ticker.replace('-', "_"));
            if matches!(
                args.storage.get_tx_from_history(&wallet_id, &args.internal_id).await,
                Ok(Some(_))
            ) {
                log::debug!("Tx '{}' already exists in tx_history. Skipping it.", &args.tx_hash);
                return Ok(None);
            } else {
                log::debug!("Adding tx: '{}' to tx_history.", &args.tx_hash);
            }

            let address = args.coin.my_address().expect("my_address can't fail");
            let tx_amounts = get_tx_amounts(coin_amount, args.coin.decimals(), htlc_tx.sender == address)
                .map_err(StopReason::Marshaling)?;

            Ok(Some(TransactionDetails {
                from: vec![htlc_tx.sender],
                to: vec![htlc_tx.to.clone()],
                total_amount: tx_amounts.total,
                spent_by_me: tx_amounts.spent_by_me,
                received_by_me: tx_amounts.received_by_me,
                my_balance_change: tx_amounts.my_balance_change,
                tx_hash: args.tx_hash,
                tx_hex: args.tx_body_msg.into(),
                fee_details: Some(args.fee_details),
                block_height: args.block_height,
                coin: tx_coin_ticker.to_string(),
                internal_id: args.internal_id,
                timestamp: common::now_ms() / 1000,
                kmd_rewards: None,
                transaction_type: Default::default(),
            }))
        }

        async fn fetch_and_insert_txs<Coin, Storage>(
            coin: &Coin,
            storage: &Storage,
            query: String,
            active_assets: &HashMap<String, String>,
        ) -> Result<(), Stopped<Coin, Storage>>
        where
            Coin: TendermintTxHistoryOps,
            Storage: TxHistoryStorage,
        {
            let mut page = 1;
            let mut iterate_more = true;

            let client = try_or_return_stopped_as_err!(
                coin.get_rpc_client().await,
                StopReason::RpcClient,
                "could not get rpc client"
            );
            while iterate_more {
                let response = try_or_return_stopped_as_err!(
                    client
                        .perform(TxSearchRequest::new(
                            query.clone(),
                            false,
                            page,
                            TX_PAGE_SIZE,
                            TendermintResultOrder::Ascending.into(),
                        ))
                        .await,
                    StopReason::RpcClient,
                    "tx search rpc call failed"
                );

                let mut tx_details: HashMap<String, Vec<TransactionDetails>> = HashMap::new();
                for tx in response.txs {
                    let internal_id = H256::from(tx.hash.as_bytes()).reversed().to_vec().into();

                    let deserialized_tx = try_or_return_stopped_as_err!(
                        cosmrs::Tx::from_bytes(tx.tx.as_bytes()),
                        StopReason::Marshaling,
                        "Could not deserialize transaction"
                    );

                    let fee_details = try_or_return_stopped_as_err!(
                        get_fee_details(deserialized_tx.auth_info.fee, coin),
                        StopReason::Marshaling,
                        "get_fee_details failed"
                    );

                    let msg = try_or_return_stopped_as_err!(
                        deserialized_tx.body.messages.first().ok_or("Tx body couldn't be read."),
                        StopReason::Marshaling,
                        "Tx body messages is empty"
                    );

                    // If we don't get expected type_url, then continue.
                    // (This check exists for not allocating ParseTxDetailsArgs when we don't use
                    // it. We could use it in match statement but that way we have to duplicate ~10
                    // lines of code for each pattern.)
                    if ![SEND_TYPE_URL, CLAIM_HTLC_TYPE_URL, CREATE_HTLC_TYPE_URL].contains(&msg.type_url.as_str()) {
                        continue;
                    }

                    let args = ParseTxDetailsArgs {
                        coin,
                        storage,
                        active_assets,
                        internal_id,
                        block_height: tx.height.into(),
                        tx_hash: tx.hash.to_string(),
                        tx_body_msg: msg.value.as_slice(),
                        fee_details,
                    };

                    let details = match msg.type_url.as_str() {
                        SEND_TYPE_URL => match parse_send_tx_details(args).await {
                            Ok(details) => details,
                            Err(e) => {
                                return Err(Stopped {
                                    phantom: Default::default(),
                                    stop_reason: e,
                                });
                            },
                        },
                        CLAIM_HTLC_TYPE_URL => match parse_claim_htlc_tx_details(args).await {
                            Ok(details) => details,
                            Err(e) => {
                                return Err(Stopped {
                                    phantom: Default::default(),
                                    stop_reason: e,
                                });
                            },
                        },
                        CREATE_HTLC_TYPE_URL => match parse_create_htlc_tx_details(args).await {
                            Ok(details) => details,
                            Err(e) => {
                                return Err(Stopped {
                                    phantom: Default::default(),
                                    stop_reason: e,
                                });
                            },
                        },
                        _ => continue,
                    };

                    let details = match details {
                        Some(details) => details,
                        None => continue,
                    };

                    upsert_tx_details(&mut tx_details, details);
                }

                for (ticker, txs) in tx_details.into_iter() {
                    let id = WalletId::new(ticker.replace('-', "_"));

                    try_or_return_stopped_as_err!(
                        storage
                            .add_transactions_to_history(&id, txs)
                            .await
                            .map_err(|e| format!("{:?}", e)),
                        StopReason::StorageError,
                        "add_transactions_to_history failed"
                    );
                }

                iterate_more = (TX_PAGE_SIZE as u32 * page) < response.total_count;
                page += 1;
            }

            Ok(())
        }

        let q = format!("transfer.sender = '{}'", self.address.clone());
        if let Err(stopped) = fetch_and_insert_txs(&ctx.coin, &ctx.storage, q, &self.active_assets).await {
            return Self::change_state(stopped);
        };

        let q = format!("transfer.recipient = '{}'", self.address.clone());
        if let Err(stopped) = fetch_and_insert_txs(&ctx.coin, &ctx.storage, q, &self.active_assets).await {
            return Self::change_state(stopped);
        };

        log::info!("Tx history fetching finished for tendermint");
        ctx.coin.set_history_sync_state(HistorySyncState::Finished);
        Self::change_state(WaitForHistoryUpdateTrigger::new())
    }
}

async fn ensure_storage_init<Storage>(wallet_ids: &[WalletId], storage: &Storage) -> MmResult<(), String>
where
    Storage: TxHistoryStorage,
{
    for wallet_id in wallet_ids.iter() {
        if let Err(e) = storage.init(wallet_id).await {
            return MmError::err(format!("{:?}", e));
        }
    }

    Ok(())
}

#[async_trait]
impl<Coin, Storage> State for TendermintInit<Coin, Storage>
where
    Coin: TendermintTxHistoryOps,
    Storage: TxHistoryStorage,
{
    type Ctx = TendermintTxHistoryCtx<Coin, Storage>;
    type Result = ();

    async fn on_changed(self: Box<Self>, ctx: &mut Self::Ctx) -> StateResult<Self::Ctx, Self::Result> {
        ctx.coin.set_history_sync_state(HistorySyncState::NotStarted);

        if let Err(e) = ensure_storage_init(&ctx.coin.all_wallet_ids(), &ctx.storage).await {
            return Self::change_state(Stopped::storage_error(e));
        }

        Self::change_state(FetchingTransactionsData::new(
            ctx.coin.my_address().expect("my_address can't fail"),
            ctx.coin.get_denom_and_ticker_map(),
        ))
    }
}

#[async_trait]
impl<Coin, Storage> LastState for Stopped<Coin, Storage>
where
    Coin: TendermintTxHistoryOps,
    Storage: TxHistoryStorage,
{
    type Ctx = TendermintTxHistoryCtx<Coin, Storage>;
    type Result = ();

    async fn on_changed(self: Box<Self>, ctx: &mut Self::Ctx) -> Self::Result {
        log::info!(
            "Stopping tx history fetching for {}. Reason: {:?}",
            ctx.coin.ticker(),
            self.stop_reason
        );

        let new_state_json = json!({
            "message": format!("{:?}", self.stop_reason),
        });

        ctx.coin.set_history_sync_state(HistorySyncState::Error(new_state_json));
    }
}

#[async_trait]
impl TendermintTxHistoryOps for TendermintCoin {
    async fn get_rpc_client(&self) -> MmResult<HttpClient, TendermintCoinRpcError> { self.rpc_client().await }

    async fn query_htlc(&self, id: String) -> MmResult<QueryHtlcResponseProto, TendermintCoinRpcError> {
        self.query_htlc(id).await
    }

    fn decimals(&self) -> u8 { self.decimals }

    fn set_history_sync_state(&self, new_state: HistorySyncState) {
        *self.history_sync_state.lock().unwrap() = new_state;
    }

    async fn all_balances(&self) -> MmResult<AllBalancesResult, TendermintCoinRpcError> { self.all_balances().await }

    fn all_wallet_ids(&self) -> Vec<WalletId> {
        let tokens = self.tokens_info.lock();
        let mut ids = vec![];

        ids.push(WalletId::new(self.ticker().replace('-', "_")));

        for (ticker, _info) in tokens.iter() {
            ids.push(WalletId::new(ticker.replace('-', "_")));
        }

        ids
    }

    fn get_denom_and_ticker_map(&self) -> HashMap<String, String> {
        let tokens = self.tokens_info.lock();

        let mut map = HashMap::with_capacity(tokens.len() + 1);
        map.insert(
            self.denom.to_string().to_lowercase(),
            self.ticker().to_string().to_uppercase(),
        );

        for (ticker, info) in tokens.iter() {
            map.insert(info.denom.to_string().to_lowercase(), ticker.to_uppercase());
        }

        map
    }
}

pub async fn tendermint_history_loop(
    coin: TendermintCoin,
    storage: impl TxHistoryStorage,
    metrics: MetricsArc,
    _current_balance: BigDecimal,
) {
    let balances = match coin.all_balances().await {
        Ok(balances) => balances,
        Err(e) => {
            log::error!("{}", e);
            return;
        },
    };

    let ctx = TendermintTxHistoryCtx {
        coin,
        storage,
        metrics,
        balances,
    };

    let state_machine: StateMachine<_, ()> = StateMachine::from_ctx(ctx);
    state_machine.run(TendermintInit::new()).await;
}
