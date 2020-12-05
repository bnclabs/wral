use std::{ffi, path};

use crate::Error;

pub fn make_filename(name: String, num: usize) -> ffi::OsString {
    let file = format!("{}-journal-{:03}.dat", name, num);
    let file: &ffi::OsStr = file.as_ref();
    file.to_os_string()
}

pub fn unwrap_filename(file: ffi::OsString) -> Option<(String, usize)> {
    let stem = {
        let fname = path::Path::new(path::Path::new(&file).file_name()?);
        match fname.extension()?.to_str()? {
            "dat" => Some(fname.file_stem()?.to_str()?.to_string()),
            _ => None,
        }?
    };

    let mut parts: Vec<&str> = stem.split('-').collect();

    let (name, parts) = match parts.len() {
        3 => Some((parts.remove(0).to_string(), parts)),
        n if n > 3 => {
            let name: Vec<&str> = parts.drain(..n - 2).collect();
            Some((name.join("-"), parts))
        }
        _ => None,
    }?;

    match parts[..] {
        ["journal", num] => {
            let num: usize = err_at!(FailConvert, num.parse()).ok()?;
            Some((name, num))
        }
        _ => None,
    }
}
