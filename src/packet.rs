use crate::error::DeltaError;

const MAGIC: [u8; 2] = *b"DS";
pub const MIN_WIRE_VERSION: u8 = 2;
pub const WIRE_VERSION: u8 = 3;
const HEADER_LEN: usize = 46;

pub const FLAG_COMPRESSED_ZSTD: u16 = 1 << 0;
pub const KNOWN_FLAGS: u16 = FLAG_COMPRESSED_ZSTD;
pub const MAX_WIRE_PAYLOAD: usize = 64 * 1024 * 1024;
pub const MAX_LOGICAL_PAYLOAD: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketKind {
    Snapshot = 1,
    Delta = 2,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Packet {
    pub kind: PacketKind,
    pub flags: u16,
    pub sequence: u64,
    pub base_sequence: u64,
    pub base_hash: u64,
    pub schema_hash: u64,
    pub payload: Vec<u8>,
}

impl Packet {
    pub fn snapshot(sequence: u64, state_hash: u64, schema_hash: u64, payload: Vec<u8>) -> Self {
        Self {
            kind: PacketKind::Snapshot,
            flags: 0,
            sequence,
            base_sequence: 0,
            base_hash: state_hash,
            schema_hash,
            payload,
        }
    }

    pub fn delta(
        sequence: u64,
        base_sequence: u64,
        base_hash: u64,
        schema_hash: u64,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            kind: PacketKind::Delta,
            flags: 0,
            sequence,
            base_sequence,
            base_hash,
            schema_hash,
            payload,
        }
    }

    pub fn encoded_len(&self) -> usize {
        HEADER_LEN + self.payload.len()
    }

    pub fn is_compressed(&self) -> bool {
        self.flags & FLAG_COMPRESSED_ZSTD != 0
    }

    pub fn payload_checksum(&self) -> u32 {
        crc32fast::hash(&self.payload)
    }

    /// Return a zstd-compressed copy when it is materially smaller.
    /// The original packet is returned when compression does not pay for itself.
    pub fn zstd_candidate(&self, level: i32, min_savings: usize) -> Result<Self, DeltaError> {
        if self.is_compressed() || self.payload.len() < 64 {
            return Ok(self.clone());
        }
        let compressed = zstd::bulk::compress(&self.payload, level)
            .map_err(|e| DeltaError::Compression(e.to_string()))?;
        if compressed.len().saturating_add(min_savings) >= self.payload.len() {
            return Ok(self.clone());
        }
        let mut packet = self.clone();
        packet.payload = compressed;
        packet.flags |= FLAG_COMPRESSED_ZSTD;
        Ok(packet)
    }

    pub fn logical_payload(&self) -> Result<Vec<u8>, DeltaError> {
        if !self.is_compressed() {
            if self.payload.len() > MAX_LOGICAL_PAYLOAD {
                return Err(DeltaError::ResourceLimit("logical payload exceeds 64 MiB"));
            }
            return Ok(self.payload.clone());
        }
        zstd::bulk::decompress(&self.payload, MAX_LOGICAL_PAYLOAD)
            .map_err(|e| DeltaError::Compression(e.to_string()))
    }

    /// Serializes this packet into the DeltaStream wire format.
    pub fn to_bytes(&self) -> Result<Vec<u8>, DeltaError> {
        self.encode()
    }

    /// Decodes a packet from the DeltaStream wire format.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DeltaError> {
        Self::decode(bytes)
    }

    /// Serializes this packet into the DeltaStream wire format.
    pub fn encode(&self) -> Result<Vec<u8>, DeltaError> {
        if self.flags & !KNOWN_FLAGS != 0 {
            return Err(DeltaError::InvalidPacket("unknown packet flags".into()));
        }
        if self.payload.len() > MAX_WIRE_PAYLOAD {
            return Err(DeltaError::ResourceLimit("wire payload exceeds 64 MiB"));
        }
        let payload_len = u32::try_from(self.payload.len())
            .map_err(|_| DeltaError::InvalidPacket("payload too large".into()))?;
        let mut out = Vec::with_capacity(self.encoded_len());
        out.extend_from_slice(&MAGIC);
        out.push(WIRE_VERSION);
        out.push(self.kind as u8);
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.extend_from_slice(&self.sequence.to_le_bytes());
        out.extend_from_slice(&self.base_sequence.to_le_bytes());
        out.extend_from_slice(&self.base_hash.to_le_bytes());
        out.extend_from_slice(&self.schema_hash.to_le_bytes());
        out.extend_from_slice(&self.payload_checksum().to_le_bytes());
        out.extend_from_slice(&payload_len.to_le_bytes());
        out.extend_from_slice(&self.payload);
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DeltaError> {
        if bytes.len() < HEADER_LEN {
            return Err(DeltaError::InvalidPacket(
                "packet shorter than header".into(),
            ));
        }
        if bytes[0..2] != MAGIC {
            return Err(DeltaError::InvalidPacket("bad magic".into()));
        }
        let version = bytes[2];
        if !(MIN_WIRE_VERSION..=WIRE_VERSION).contains(&version) {
            return Err(DeltaError::ProtocolVersionRange {
                received: version,
                min_supported: MIN_WIRE_VERSION,
                max_supported: WIRE_VERSION,
            });
        }
        let kind = match bytes[3] {
            1 => PacketKind::Snapshot,
            2 => PacketKind::Delta,
            value => {
                return Err(DeltaError::InvalidPacket(format!(
                    "unknown packet kind {value}"
                )))
            }
        };
        let flags = u16::from_le_bytes(bytes[4..6].try_into().expect("fixed slice"));
        if flags & !KNOWN_FLAGS != 0 {
            return Err(DeltaError::InvalidPacket("unknown packet flags".into()));
        }
        let sequence = u64::from_le_bytes(bytes[6..14].try_into().expect("fixed slice"));
        let base_sequence = u64::from_le_bytes(bytes[14..22].try_into().expect("fixed slice"));
        let base_hash = u64::from_le_bytes(bytes[22..30].try_into().expect("fixed slice"));
        let schema_hash = u64::from_le_bytes(bytes[30..38].try_into().expect("fixed slice"));
        let checksum = u32::from_le_bytes(bytes[38..42].try_into().expect("fixed slice"));
        let payload_len =
            u32::from_le_bytes(bytes[42..46].try_into().expect("fixed slice")) as usize;
        if payload_len > MAX_WIRE_PAYLOAD {
            return Err(DeltaError::ResourceLimit("wire payload exceeds 64 MiB"));
        }
        if bytes.len() != HEADER_LEN + payload_len {
            return Err(DeltaError::InvalidPacket("payload length mismatch".into()));
        }
        let payload = bytes[HEADER_LEN..].to_vec();
        let actual = crc32fast::hash(&payload);
        if actual != checksum {
            return Err(DeltaError::ChecksumMismatch {
                expected: checksum,
                actual,
            });
        }
        Ok(Self {
            kind,
            flags,
            sequence,
            base_sequence,
            base_hash,
            schema_hash,
            payload,
        })
    }
}
