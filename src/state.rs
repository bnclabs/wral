use mkit::{
    cbor::{FromCbor, IntoCbor},
    Cborize,
};

use crate::{entry::Entry, Result};

pub trait State: 'static + Clone + Sync + Send + IntoCbor + FromCbor {
    fn on_add_entry(&mut self, new_entry: &Entry) -> Result<()>;
}

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
