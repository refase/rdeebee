use std::{fs::File, io::BufWriter, path::PathBuf};

pub(crate) struct Wal {
    path: PathBuf,
    file: BufWriter<File>,
}

impl Wal {
    const WAL_NAME: &str = "rdeebee";
}
