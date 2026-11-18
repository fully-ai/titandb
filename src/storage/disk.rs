use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::error::Result;
use crate::storage::page::{PAGE_SIZE, SlottedPage};

/// Manages durable 4KB slotted-page file I/O and page allocation.
pub struct DiskManager {
    path: PathBuf,
    file: Mutex<File>,
    num_pages: Mutex<u32>,
}

impl DiskManager {
    /// Opens or creates a TitanDB paged data file.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
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
        let len = file.metadata()?.len();
        let num_pages = (len / PAGE_SIZE as u64) as u32;
        Ok(Self {
            path: p,
            file: Mutex::new(file),
            num_pages: Mutex::new(num_pages),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn page_count(&self) -> u32 {
        *self.num_pages.lock().unwrap()
    }

    /// Allocates and initializes a new CRC32-verified empty page on disk.
    pub fn allocate_page(&self) -> Result<SlottedPage> {
        let mut cnt = self.num_pages.lock().unwrap();
        let page_id = *cnt;
        *cnt += 1;
        let page = SlottedPage::new(page_id);
        drop(cnt);
        self.write_page(&page)?;
        Ok(page)
    }

    /// Reads page_id from disk and verifies its magic + CRC32 checksum.
    pub fn read_page(&self, page_id: u32) -> Result<SlottedPage> {
        let mut f = self.file.lock().unwrap();
        f.seek(SeekFrom::Start((page_id as u64) * (PAGE_SIZE as u64)))?;
        let mut buf = [0u8; PAGE_SIZE];
        f.read_exact(&mut buf)?;
        SlottedPage::from_bytes(buf)
    }

    /// Writes a page cleanly (recomputing checksum before writing).
    pub fn write_page(&self, page: &SlottedPage) -> Result<()> {
        let mut copy = page.clone();
        copy.update_checksum();
        let pid = copy.page_id();
        let mut f = self.file.lock().unwrap();
        let mut cnt = self.num_pages.lock().unwrap();
        if pid >= *cnt {
            *cnt = pid + 1;
        }
        drop(cnt);
        f.seek(SeekFrom::Start((pid as u64) * (PAGE_SIZE as u64)))?;
        f.write_all(&copy.data)?;
        Ok(())
    }

    /// Flushes OS file buffers to physical storage.
    pub fn sync(&self) -> Result<()> {
        let f = self.file.lock().unwrap();
        f.sync_data()?;
        Ok(())
    }
}
