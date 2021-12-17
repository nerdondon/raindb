//! Utilities for checksums.

/// A constant delta for masking and unmasking checksums.
const CRC_MASKING_DELTA: u32 = 0xa282ead8;

/**
Return a masked representation of the checksum.

# Motivation

Per LevelDB, computing the CRC of a string that contains embedded CRC's can be problematic. So they
recommend masking CRC's that will be stored somewhere (e.g. in a file).
*/
pub(crate) fn mask_checksum(checksum: u32) -> u32 {
    // Rotate right by 15 bits and add a constant.
    ((checksum >> 15) | (checksum << 17)) + CRC_MASKING_DELTA
}

/**
Return the unmasked checksum.

The checksum must have been masked with [`crate::utils::crc::mask_checksum`].
*/
pub(crate) fn unmask_checksum(masked_checksum: u32) -> u32 {
    let rotated = masked_checksum - CRC_MASKING_DELTA;
    (rotated >> 17) | (rotated << 15)
}
