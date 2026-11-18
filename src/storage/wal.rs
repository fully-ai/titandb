use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crc32fast::Hasher;
use serde::{Deserialize, Serialize};

use crate::error::{Result, TitanError};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WalOp {
    Begin,
    Commit,
    Abort,
    Checkpoint,
    InsertTuple {
        table: String,
        row_id: u64,
        row_json: Vec<u8>,
    },
    UpdateTuple {
        table: String,
        row_id: u64,
        old_json: Vec<u8>,
        new_json: Vec<u8>,
    },
    DeleteTuple {
        table: String,
        row_id: u64,
        old_json: Vec<u8>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalRecord {
    pub lsn: u64,
    pub txn_id: u64,
    pub op: WalOp,
}

/// Durable CRC32-framed binary Write-Ahead Log manager supporting crash recovery.
///
/// Frame format: [payload_len: u32 LE][crc32: u32 LE][json_payload...]
pub struct WalManager {
    path: PathBuf,
    file: Mutex<File>,
    next_lsn: Mutex<u64>,
    sync_on_commit: bool,
}

impl WalManager {
    pub fn open<P: AsRef<Path>>(path: P, sync_on_commit: bool) -> Result<Self> {
        let p = path.as_ref().to_path_buf();
        if let Some(parent) = p.parent()
            && !parent.as_os_str().is_empty()
        {
            std::fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&p)?;
        let mgr = Self {
            path: p,
            file: Mutex::new(file),
            next_lsn: Mutex::new(1),
            sync_on_commit,
        };
        let existing = mgr.read_all()?;
        if let Some(last) = existing.last() {
            *mgr.next_lsn.lock().unwrap() = last.lsn + 1;
        }
        Ok(mgr)
    }

    pub fn append(&self, txn_id: u64, op: WalOp) -> Result<u64> {
        let mut lsn_guard = self.next_lsn.lock().unwrap();
        let lsn = *lsn_guard;
        *lsn_guard += 1;
        drop(lsn_guard);

        let rec = WalRecord {
            lsn,
            txn_id,
            op: op.clone(),
        };
        let payload = serde_json::to_vec(&rec)?;
        let mut h = Hasher::new();
        h.update(&payload);
        let crc = h.finalize();

        let mut frame = Vec::with_capacity(8 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        frame.extend_from_slice(&crc.to_le_bytes());
        frame.extend_from_slice(&payload);

        let mut f = self.file.lock().unwrap();
        f.seek(SeekFrom::End(0))?;
        f.write_all(&frame)?;
        if self.sync_on_commit && matches!(op, WalOp::Commit | WalOp::Checkpoint) {
            f.sync_data()?;
        }
        Ok(lsn)
    }

    pub fn sync(&self) -> Result<()> {
        self.file.lock().unwrap().sync_data()?;
        Ok(())
    }

    /// Reads all valid WAL frames, cleanly stopping on incomplete torn tail write or reporting corruption if internal frame CRC mismatches.
    pub fn read_all(&self) -> Result<Vec<WalRecord>> {
        let mut f = self.file.lock().unwrap();
        f.seek(SeekFrom::Start(0))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;

        let mut pos = 0usize;
        let mut records = Vec::new();
        while pos + 8 <= buf.len() {
            let len = u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;
            let expected_crc = u32::from_le_bytes(buf[pos + 4..pos + 8].try_into().unwrap());
            pos += 8;
            if len == 0 || pos + len > buf.len() {
                // Incomplete tail record after sudden power loss: stop replay cleanly at last intact frame
                break;
            }
            let payload = &buf[pos..pos + len];
            pos += len;
            let mut h = Hasher::new();
            h.update(payload);
            let actual_crc = h.finalize();
            if actual_crc != expected_crc {
                return Err(TitanError::Corruption(format!(
                    "WAL frame CRC mismatch (expected 0x{expected_crc:08x}, got 0x{actual_crc:08x})"
                )));
            }
            let rec: WalRecord = serde_json::from_slice(payload)?;
            records.push(rec);
        }
        Ok(records)
    }

    /// Truncates the WAL cleanly after pages/state have been checkpointed safely.
    pub fn truncate_after_checkpoint(&self) -> Result<()> {
        let mut f = self.file.lock().unwrap();
        f.set_len(0)?;
        f.seek(SeekFrom::Start(0))?;
        f.sync_data()?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}
