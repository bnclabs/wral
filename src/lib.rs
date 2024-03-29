//! Package implement Write-Ahead-Logging.
//!
//! Write-Ahead-Logging is implemented by [Wal] type, to get started create
//! a configuration [Config] value. Subsequently, a fresh Wal instance can be
//! created or existing Wal from disk can be loaded, using the configuration.
//! Wal optionally takes a type parameter `S` for state, that can be used by
//! application to persist storage state along with each batch.
//! By default, `NoState` is used.
//!
//! Concurrent writers
//! ------------------
//!
//! [Wal] writes are batch-processed, where batching is automatically dictated
//! by storage (disk, ssd) latency. Latency can get higher when `fsync` is
//! enabled for every batch flush. With fsync enabled it is hard to reduce
//! the latency, and to get better throughput applications can do concurrent
//! writes. This is possible because [Wal] type can be cloned with underlying
//! structure safely shared among all the clones. For example,
//!
//! ```ignore
//! let wal = wral::Wal::create(config, wral::NoState).unwrap();
//! let mut writers = vec![];
//! for id in 0..n_threads {
//!     let wal = wal.clone();
//!     writers.push(std::thread::spawn(move || writer(id, wal)));
//! }
//! ```
//!
//! Application employing concurrent [Wal] must keep in mind that `seqno`
//! generated for consecutive ops may not be monotonically increasing within
//! the same thread, and must make sure to serialize operations across the
//! writers through other means.
//!
//! Concurrent readers
//! ------------------
//!
//! It is possible for a [Wal] value and its clones to concurrently read the
//! log journal (typically iterating over its entries). Remember that read
//! operations shall block concurrent writes and vice-versa. But concurrent
//! reads shall be allowed.

use std::{error, fmt, result};

// Short form to compose Error values.
//
// Here are few possible ways:
//
// ```ignore
// use crate::Error;
// err_at!(ParseError, msg: format!("bad argument"));
// ```
//
// ```ignore
// use crate::Error;
// err_at!(ParseError, std::io::read(buf));
// ```
//
// ```ignore
// use crate::Error;
// err_at!(ParseError, std::fs::read(file_path), format!("read failed"));
// ```
//
macro_rules! err_at {
    ($v:ident, msg: $($arg:expr),+) => {{
        let prefix = format!("{}:{}", file!(), line!());
        Err(Error::$v(prefix, format!($($arg),+)))
    }};
    ($v:ident, $e:expr) => {{
        match $e {
            Ok(val) => Ok(val),
            Err(err) => {
                let prefix = format!("{}:{}", file!(), line!());
                Err(Error::$v(prefix, format!("{}", err)))
            }
        }
    }};
    ($v:ident, $e:expr, $($arg:expr),+) => {{
        match $e {
            Ok(val) => Ok(val),
            Err(err) => {
                let prefix = format!("{}:{}", file!(), line!());
                let msg = format!($($arg),+);
                Err(Error::$v(prefix, format!("{} {}", err, msg)))
            }
        }
    }};
}

mod batch;
mod entry;
mod files;
mod journal;
mod state;
mod util;
mod wral;
mod writer;

pub use crate::entry::Entry;
pub use crate::state::{NoState, State};
pub use crate::wral::Config;
pub use crate::wral::Wal;

/// Type alias for Result return type, used by this package.
pub type Result<T> = result::Result<T, Error>;

/// Error variants that can be returned by this package's API.
///
/// Each variant carries a prefix, typically identifying the
/// error location.
pub enum Error {
    FailConvert(String, String),
    FailCbor(String, String),
    IOError(String, String),
    Fatal(String, String),
    Invalid(String, String),
    IPCFail(String, String),
    ThreadFail(String, String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        use Error::*;

        match self {
            FailConvert(p, msg) => write!(f, "{} FailConvert: {}", p, msg),
            FailCbor(p, msg) => write!(f, "{} FailCbor: {}", p, msg),
            IOError(p, msg) => write!(f, "{} IOError: {}", p, msg),
            Fatal(p, msg) => write!(f, "{} Fatal: {}", p, msg),
            Invalid(p, msg) => write!(f, "{} Invalid: {}", p, msg),
            IPCFail(p, msg) => write!(f, "{} IPCFail: {}", p, msg),
            ThreadFail(p, msg) => write!(f, "{} ThreadFail: {}", p, msg),
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> result::Result<(), fmt::Error> {
        write!(f, "{}", self)
    }
}

impl error::Error for Error {}

impl From<mkit::Error> for Error {
    fn from(err: mkit::Error) -> Error {
        match err {
            mkit::Error::Fatal(p, m) => Error::Fatal(p, m),
            mkit::Error::FailConvert(p, m) => Error::FailConvert(p, m),
            mkit::Error::IOError(p, m) => Error::IOError(p, m),
            mkit::Error::FailCbor(p, m) => Error::FailCbor(p, m),
            mkit::Error::IPCFail(p, m) => Error::IPCFail(p, m),
            mkit::Error::ThreadFail(p, m) => Error::ThreadFail(p, m),
        }
    }
}
