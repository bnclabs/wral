use crate::{entry::Entry, Result};

pub trait State {
    fn on_add_entry(&mut self, new_entry: &Entry) -> Result<()>;
}

pub struct NoState;

impl State for NoState {
    fn on_add_entry(&mut self, _: &Entry) -> Result<()> {
        Ok(())
    }
}
