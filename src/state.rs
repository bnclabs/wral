use mkit::{
    cbor::{FromCbor, IntoCbor},
    Cborize,
};

#[allow(unused_imports)]
use crate::wral::Wral;
use crate::{entry::Entry, Result};

/// Callback trait for updating application state in relation to [Wral] type.
pub trait State: 'static + Clone + Sync + Send + IntoCbor + FromCbor {
    fn on_add_entry(&mut self, new_entry: &Entry) -> Result<()>;
}

/// Default parameter, implementing [State] trait, for [Wral] type.
#[derive(Clone, Eq, PartialEq, Debug, Cborize)]
pub struct NoState;

impl NoState {
    const ID: u64 = 0x0;
}

impl State for NoState {
    fn on_add_entry(&mut self, _: &Entry) -> Result<()> {
        Ok(())
    }
}
