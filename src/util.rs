use mkit::{self, cbor::Cbor};

use std::{convert::TryInto, fs, io::Write};

use crate::{Error, Result};

pub fn encode_cbor<T>(val: T) -> Result<Vec<u8>>
where
    T: TryInto<Cbor, Error = mkit::Error>,
{
    let mut data: Vec<u8> = vec![];
    let n = val.try_into()?.encode(&mut data)?;
    if n != data.len() {
        err_at!(Fatal, msg: "cbor encoding len mistmatch {} {}", n, data.len())
    } else {
        Ok(data)
    }
}

pub fn sync_write(file: &mut fs::File, data: &[u8]) -> Result<usize> {
    let n = err_at!(IOError, file.write(data))?;
    if n != data.len() {
        err_at!(IOError, msg: "partial write to file {} {}", n, data.len())?
    }
    err_at!(IOError, file.sync_all())?;
    Ok(n)
}
