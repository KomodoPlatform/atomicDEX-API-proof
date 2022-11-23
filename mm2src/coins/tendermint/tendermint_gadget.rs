use super::{AccountIdFromPubkeyHexErr, SearchForSwapTxSpendErr, TendermintCoin, TendermintCoinImpl,
            TendermintCoinRpcError, TendermintToken, TendermintTokenImpl, TendermintTokenInitError};

use crate::{coin_errors::{MyAddressError, ValidatePaymentError},
            BalanceError, RawTransactionError, TradePreimageError, WithdrawError};
use common::executor::AbortedError;
use cosmrs::ErrorReport;
use hex::FromHexError;
use prost::DecodeError;
use std::ops::Deref;

////////////////////////////////////////
// implementations of tendermint_coin //
////////////////////////////////////////

impl From<DecodeError> for TendermintCoinRpcError {
    fn from(err: DecodeError) -> Self { TendermintCoinRpcError::Prost(err) }
}

impl From<TendermintCoinRpcError> for WithdrawError {
    fn from(err: TendermintCoinRpcError) -> Self { WithdrawError::Transport(err.to_string()) }
}

impl From<TendermintCoinRpcError> for BalanceError {
    fn from(err: TendermintCoinRpcError) -> Self {
        match err {
            TendermintCoinRpcError::InvalidResponse(e) => BalanceError::InvalidResponse(e),
            TendermintCoinRpcError::Prost(e) => BalanceError::InvalidResponse(e.to_string()),
            TendermintCoinRpcError::PerformError(e) => BalanceError::Transport(e),
        }
    }
}

impl From<TendermintCoinRpcError> for ValidatePaymentError {
    fn from(err: TendermintCoinRpcError) -> Self {
        match err {
            TendermintCoinRpcError::InvalidResponse(e) => ValidatePaymentError::InvalidRpcResponse(e),
            TendermintCoinRpcError::Prost(e) => ValidatePaymentError::InvalidRpcResponse(e.to_string()),
            TendermintCoinRpcError::PerformError(e) => ValidatePaymentError::Transport(e),
        }
    }
}

impl From<TendermintCoinRpcError> for TradePreimageError {
    fn from(err: TendermintCoinRpcError) -> Self { TradePreimageError::Transport(err.to_string()) }
}

#[cfg(not(target_arch = "wasm32"))]
impl From<tendermint_rpc::Error> for TendermintCoinRpcError {
    fn from(err: tendermint_rpc::Error) -> Self { TendermintCoinRpcError::PerformError(err.to_string()) }
}

#[cfg(target_arch = "wasm32")]
impl From<PerformError> for TendermintCoinRpcError {
    fn from(err: PerformError) -> Self { TendermintCoinRpcError::PerformError(err.to_string()) }
}

impl From<TendermintCoinRpcError> for RawTransactionError {
    fn from(err: TendermintCoinRpcError) -> Self { RawTransactionError::Transport(err.to_string()) }
}

impl From<FromHexError> for AccountIdFromPubkeyHexErr {
    fn from(err: FromHexError) -> Self { AccountIdFromPubkeyHexErr::InvalidHexString(err) }
}

impl From<ErrorReport> for AccountIdFromPubkeyHexErr {
    fn from(err: ErrorReport) -> Self { AccountIdFromPubkeyHexErr::CouldNotCreateAccountId(err) }
}

impl From<ErrorReport> for SearchForSwapTxSpendErr {
    fn from(e: ErrorReport) -> Self { SearchForSwapTxSpendErr::Cosmrs(e) }
}

impl From<TendermintCoinRpcError> for SearchForSwapTxSpendErr {
    fn from(e: TendermintCoinRpcError) -> Self { SearchForSwapTxSpendErr::Rpc(e) }
}

impl From<DecodeError> for SearchForSwapTxSpendErr {
    fn from(e: DecodeError) -> Self { SearchForSwapTxSpendErr::Proto(e) }
}

impl Deref for TendermintCoin {
    type Target = TendermintCoinImpl;

    fn deref(&self) -> &Self::Target { &self.0 }
}

/////////////////////////////////////////
// implementations of tendermint_token //
/////////////////////////////////////////

impl Deref for TendermintToken {
    type Target = TendermintTokenImpl;

    fn deref(&self) -> &Self::Target { &self.0 }
}

impl From<MyAddressError> for TendermintTokenInitError {
    fn from(err: MyAddressError) -> Self { TendermintTokenInitError::MyAddressError(err.to_string()) }
}

impl From<AbortedError> for TendermintTokenInitError {
    fn from(e: AbortedError) -> Self { TendermintTokenInitError::Internal(e.to_string()) }
}
