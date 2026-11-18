use crc32fast::Hasher;

use crate::error::{Result, TitanError};

pub const PAGE_SIZE: usize = 4096;
pub const PAGE_MAGIC: u32 = 0x5449_544E; // "TITN"
const HEADER_SIZE: usize = 24;
const SLOT_SIZE: usize = 6; // offset:u16 + len:u16 + flags:u16
const FLAG_TOMBSTONE: u16 = 0x0001;

/// Fixed 4096-byte slotted disk page with CRC32 corruption verification.
///
/// Layout:
/// [0..4]   magic (u32 LE)
/// [4..8]   page_id (u32 LE)
/// [8..16]  lsn (u64 LE)
/// [16..20] checksum (u32 LE over all bytes except [16..20])
/// [20..22] slot_count (u16 LE)
/// [22..24] free_space_ptr (u16 LE, grows downward from PAGE_SIZE)
/// [24..]   slots array -> ... free space ... <- tuple payloads
#[derive(Clone)]
pub struct SlottedPage {
    pub data: [u8; PAGE_SIZE],
}

impl SlottedPage {
    /// Creates an initialized empty slotted page with valid header & checksum.
    pub fn new(page_id: u32) -> Self {
        let mut p = Self {
            data: [0u8; PAGE_SIZE],
        };
        p.set_u32(0, PAGE_MAGIC);
        p.set_u32(4, page_id);
        p.set_u64(8, 0);
        p.set_u16(20, 0);
        p.set_u16(22, PAGE_SIZE as u16);
        p.update_checksum();
        p
    }

    pub fn from_bytes(bytes: [u8; PAGE_SIZE]) -> Result<Self> {
        let page = Self { data: bytes };
        page.verify()?;
        Ok(page)
    }

    pub fn page_id(&self) -> u32 {
        self.get_u32(4)
    }

    pub fn lsn(&self) -> u64 {
        self.get_u64(8)
    }

    pub fn set_lsn(&mut self, lsn: u64) {
        self.set_u64(8, lsn);
    }

    pub fn slot_count(&self) -> u16 {
        self.get_u16(20)
    }

    fn free_ptr(&self) -> u16 {
        self.get_u16(22)
    }

    pub fn free_bytes(&self) -> usize {
        let used_header = HEADER_SIZE + (self.slot_count() as usize) * SLOT_SIZE;
        let fptr = self.free_ptr() as usize;
        fptr.saturating_sub(used_header)
    }

    /// Computes and stores the CRC32 checksum across the 4KB page.
    pub fn update_checksum(&mut self) {
        let sum = self.compute_checksum();
        self.set_u32(16, sum);
    }

    fn compute_checksum(&self) -> u32 {
        let mut h = Hasher::new();
        h.update(&self.data[0..16]);
        h.update(&[0u8; 4]);
        h.update(&self.data[20..PAGE_SIZE]);
        h.finalize()
    }

    /// Verifies page magic and CRC32 integrity checksum.
    pub fn verify(&self) -> Result<()> {
        if self.get_u32(0) != PAGE_MAGIC {
            return Err(TitanError::Corruption(format!(
                "invalid page magic 0x{:08x}",
                self.get_u32(0)
            )));
        }
        let stored = self.get_u32(16);
        let computed = self.compute_checksum();
        if stored != computed {
            return Err(TitanError::Corruption(format!(
                "page {} checksum mismatch: stored 0x{:08x}, computed 0x{:08x}",
                self.page_id(),
                stored,
                computed
            )));
        }
        Ok(())
    }

    /// Inserts a tuple payload into the slotted page and returns its slot_id if enough space exists.
    pub fn insert_tuple(&mut self, payload: &[u8]) -> Option<u16> {
        if payload.len() > PAGE_SIZE - HEADER_SIZE - SLOT_SIZE {
            return None;
        }
        let count = self.slot_count();
        // Reuse tombstone slot if payload fits and slot exists, or allocate new slot.
        let need_new_slot = SLOT_SIZE;
        let avail = self.free_bytes();
        if avail < payload.len() + need_new_slot {
            self.compact();
            if self.free_bytes() < payload.len() + need_new_slot {
                return None;
            }
        }
        let new_free = (self.free_ptr() as usize) - payload.len();
        self.data[new_free..new_free + payload.len()].copy_from_slice(payload);
        self.set_u16(22, new_free as u16);

        let slot_off = HEADER_SIZE + (count as usize) * SLOT_SIZE;
        self.set_u16(slot_off, new_free as u16);
        self.set_u16(slot_off + 2, payload.len() as u16);
        self.set_u16(slot_off + 4, 0);
        self.set_u16(20, count + 1);
        self.update_checksum();
        Some(count)
    }

    /// Reads an active (non-tombstoned) tuple slice for slot_id.
    pub fn get_tuple(&self, slot_id: u16) -> Option<&[u8]> {
        if slot_id >= self.slot_count() {
            return None;
        }
        let slot_off = HEADER_SIZE + (slot_id as usize) * SLOT_SIZE;
        let off = self.get_u16(slot_off) as usize;
        let len = self.get_u16(slot_off + 2) as usize;
        let flags = self.get_u16(slot_off + 4);
        if flags & FLAG_TOMBSTONE != 0 || len == 0 || off + len > PAGE_SIZE {
            return None;
        }
        Some(&self.data[off..off + len])
    }

    /// Overwrites an existing slot's payload in-place if new payload is <= old capacity; otherwise appends or returns false.
    pub fn update_tuple(&mut self, slot_id: u16, payload: &[u8]) -> bool {
        if slot_id >= self.slot_count() {
            return false;
        }
        let slot_off = HEADER_SIZE + (slot_id as usize) * SLOT_SIZE;
        let off = self.get_u16(slot_off) as usize;
        let len = self.get_u16(slot_off + 2) as usize;
        let flags = self.get_u16(slot_off + 4);
        if flags & FLAG_TOMBSTONE != 0 {
            return false;
        }
        if payload.len() <= len && off + payload.len() <= PAGE_SIZE {
            self.data[off..off + payload.len()].copy_from_slice(payload);
            self.set_u16(slot_off + 2, payload.len() as u16);
            self.update_checksum();
            return true;
        }
        if self.free_bytes() < payload.len() {
            self.compact();
            if self.free_bytes() < payload.len() {
                return false;
            }
        }
        let new_free = (self.free_ptr() as usize) - payload.len();
        self.data[new_free..new_free + payload.len()].copy_from_slice(payload);
        self.set_u16(22, new_free as u16);
        self.set_u16(slot_off, new_free as u16);
        self.set_u16(slot_off + 2, payload.len() as u16);
        self.update_checksum();
        true
    }

    /// Marks slot_id as tombstoned (deleted).
    pub fn delete_tuple(&mut self, slot_id: u16) -> bool {
        if slot_id >= self.slot_count() {
            return false;
        }
        let slot_off = HEADER_SIZE + (slot_id as usize) * SLOT_SIZE;
        let flags = self.get_u16(slot_off + 4);
        self.set_u16(slot_off + 4, flags | FLAG_TOMBSTONE);
        self.update_checksum();
        true
    }

    /// Reclaims fragmented space from deleted or shrunk tuples without shifting slot_ids.
    pub fn compact(&mut self) {
        let count = self.slot_count() as usize;
        let mut live: Vec<(usize, Vec<u8>)> = Vec::new();
        for i in 0..count {
            let slot_off = HEADER_SIZE + i * SLOT_SIZE;
            let off = self.get_u16(slot_off) as usize;
            let len = self.get_u16(slot_off + 2) as usize;
            let flags = self.get_u16(slot_off + 4);
            if flags & FLAG_TOMBSTONE == 0 && len > 0 && off + len <= PAGE_SIZE {
                live.push((i, self.data[off..off + len].to_vec()));
            }
        }
        let mut cursor = PAGE_SIZE;
        for b in &mut self.data[HEADER_SIZE + count * SLOT_SIZE..PAGE_SIZE] {
            *b = 0;
        }
        for (idx, bytes) in live {
            cursor -= bytes.len();
            self.data[cursor..cursor + bytes.len()].copy_from_slice(&bytes);
            let slot_off = HEADER_SIZE + idx * SLOT_SIZE;
            self.set_u16(slot_off, cursor as u16);
            self.set_u16(slot_off + 2, bytes.len() as u16);
        }
        self.set_u16(22, cursor as u16);
        self.update_checksum();
    }

    fn get_u16(&self, off: usize) -> u16 {
        u16::from_le_bytes([self.data[off], self.data[off + 1]])
    }
    fn set_u16(&mut self, off: usize, v: u16) {
        self.data[off..off + 2].copy_from_slice(&v.to_le_bytes());
    }
    fn get_u32(&self, off: usize) -> u32 {
        u32::from_le_bytes([
            self.data[off],
            self.data[off + 1],
            self.data[off + 2],
            self.data[off + 3],
        ])
    }
    fn set_u32(&mut self, off: usize, v: u32) {
        self.data[off..off + 4].copy_from_slice(&v.to_le_bytes());
    }
    fn get_u64(&self, off: usize) -> u64 {
        u64::from_le_bytes(self.data[off..off + 8].try_into().unwrap())
    }
    fn set_u64(&mut self, off: usize, v: u64) {
        self.data[off..off + 8].copy_from_slice(&v.to_le_bytes());
    }
}
