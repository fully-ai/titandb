use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::error::{Result, TitanError};
use crate::storage::disk::DiskManager;
use crate::storage::page::SlottedPage;

struct Frame {
    page: SlottedPage,
    pin_count: usize,
    dirty: bool,
    referenced: bool,
}

#[derive(Debug, Default, Clone, serde::Serialize, serde::Deserialize)]
pub struct BufferStats {
    pub capacity: usize,
    pub resident_pages: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub flushes: u64,
}

/// Fixed-capacity Clock replacement buffer pool over DiskManager.
pub struct BufferPool {
    disk: Arc<DiskManager>,
    capacity: usize,
    frames: Mutex<Vec<Option<Frame>>>,
    page_table: Mutex<HashMap<u32, usize>>,
    clock_hand: Mutex<usize>,
    stats: Mutex<BufferStats>,
}

impl BufferPool {
    pub fn new(disk: Arc<DiskManager>, capacity: usize) -> Self {
        let cap = capacity.max(4);
        let mut frames = Vec::with_capacity(cap);
        for _ in 0..cap {
            frames.push(None);
        }
        Self {
            disk,
            capacity: cap,
            frames: Mutex::new(frames),
            page_table: Mutex::new(HashMap::new()),
            clock_hand: Mutex::new(0),
            stats: Mutex::new(BufferStats {
                capacity: cap,
                ..Default::default()
            }),
        }
    }

    /// Allocates a new page via DiskManager and pins it in the buffer pool.
    pub fn new_page(&self) -> Result<SlottedPage> {
        let page = self.disk.allocate_page()?;
        let pid = page.page_id();
        self.install_page(pid, page.clone(), true)?;
        Ok(page)
    }

    /// Fetches a page from the pool (or disk on miss) and increments its pin count.
    pub fn fetch_page(&self, page_id: u32) -> Result<SlottedPage> {
        {
            let mut frames = self.frames.lock().unwrap();
            let pt = self.page_table.lock().unwrap();
            if let Some(&idx) = pt.get(&page_id)
                && let Some(frame) = frames[idx].as_mut()
            {
                frame.pin_count += 1;
                frame.referenced = true;
                let mut st = self.stats.lock().unwrap();
                st.hits += 1;
                return Ok(frame.page.clone());
            }
        }
        let mut st = self.stats.lock().unwrap();
        st.misses += 1;
        drop(st);

        let page = self.disk.read_page(page_id)?;
        self.install_page(page_id, page.clone(), false)?;
        Ok(page)
    }

    /// Updates a page inside the pool, marks it dirty, and unpins it.
    pub fn unpin_page(
        &self,
        page_id: u32,
        dirty: bool,
        updated: Option<SlottedPage>,
    ) -> Result<()> {
        let mut frames = self.frames.lock().unwrap();
        let pt = self.page_table.lock().unwrap();
        let Some(&idx) = pt.get(&page_id) else {
            return Ok(());
        };
        if let Some(frame) = frames[idx].as_mut() {
            if let Some(p) = updated {
                frame.page = p;
                frame.dirty = true;
            } else if dirty {
                frame.dirty = true;
            }
            if frame.pin_count > 0 {
                frame.pin_count -= 1;
            }
        }
        Ok(())
    }

    /// Flushes all dirty resident pages and syncs DiskManager.
    pub fn flush_all(&self) -> Result<usize> {
        let mut flushed = 0usize;
        let mut frames = self.frames.lock().unwrap();
        for slot in frames.iter_mut() {
            if let Some(frame) = slot.as_mut()
                && frame.dirty
            {
                self.disk.write_page(&frame.page)?;
                frame.dirty = false;
                flushed += 1;
            }
        }
        self.disk.sync()?;
        let mut st = self.stats.lock().unwrap();
        st.flushes += flushed as u64;
        Ok(flushed)
    }

    pub fn stats(&self) -> BufferStats {
        let mut st = self.stats.lock().unwrap().clone();
        st.resident_pages = self.page_table.lock().unwrap().len();
        st
    }

    fn install_page(&self, page_id: u32, page: SlottedPage, dirty: bool) -> Result<()> {
        let mut frames = self.frames.lock().unwrap();
        let mut pt = self.page_table.lock().unwrap();

        if let Some(&idx) = pt.get(&page_id)
            && let Some(frame) = frames[idx].as_mut()
        {
            frame.pin_count += 1;
            frame.referenced = true;
            frame.dirty |= dirty;
            return Ok(());
        }

        // Empty frame check
        for (i, slot) in frames.iter_mut().enumerate() {
            if slot.is_none() {
                *slot = Some(Frame {
                    page,
                    pin_count: 1,
                    dirty,
                    referenced: true,
                });
                pt.insert(page_id, i);
                return Ok(());
            }
        }

        // Clock sweep eviction
        let mut hand = self.clock_hand.lock().unwrap();
        let max_probes = self.capacity * 2 + 1;
        for _ in 0..max_probes {
            let idx = *hand % self.capacity;
            *hand = (*hand + 1) % self.capacity;
            if let Some(frame) = frames[idx].as_mut() {
                if frame.pin_count > 0 {
                    continue;
                }
                if frame.referenced {
                    frame.referenced = false;
                    continue;
                }
                if frame.dirty {
                    self.disk.write_page(&frame.page)?;
                }
                let victim_id = frame.page.page_id();
                pt.remove(&victim_id);
                frames[idx] = Some(Frame {
                    page,
                    pin_count: 1,
                    dirty,
                    referenced: true,
                });
                pt.insert(page_id, idx);
                let mut st = self.stats.lock().unwrap();
                st.evictions += 1;
                return Ok(());
            }
        }
        Err(TitanError::Execution(
            "buffer pool exhausted: all frames pinned".into(),
        ))
    }
}
