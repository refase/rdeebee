// This is a dangerous lint.
// Just here till we develop things fully.
// Should be removed.
#![allow(dead_code)]
mod recovery;
mod storage;

#[cfg(test)]
mod tests {
    use super::*;
}
