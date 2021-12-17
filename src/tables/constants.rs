//! Contains constants that are common across table operations.

use crc::{Crc, CRC_32_ISCSI};

/**
The size of a block descriptor in bytes.

This is a 1-byte enum + a 32-bit CRC (4 bytes).
*/
pub(crate) const BLOCK_DESCRIPTOR_SIZE_BYTES: usize = 1 + 4;

/**
CRC calculator using the iSCSI polynomial.

LevelDB uses the [google/crc32c](https://github.com/google/crc32c) CRC implementation. This
implementation specifies using the iSCSI polynomial so that is what we use here as well.
*/
pub(crate) const CRC_CALCULATOR: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);
