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

#[cfg(test)]
mod tests {
    use crc::{Crc, CRC_32_ISCSI};

    use super::*;
    use pretty_assertions::{assert_eq, assert_ne};

    const CRC_CALCULATOR: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

    #[test]
    fn can_mask_and_unmask_checksums_correctly() {
        let checksum = CRC_CALCULATOR.checksum(b"foo");

        assert_ne!(checksum, mask_checksum(checksum));
        assert_ne!(checksum, mask_checksum(mask_checksum(checksum)));
        assert_eq!(checksum, unmask_checksum(mask_checksum(checksum)));
        assert_eq!(
            checksum,
            unmask_checksum(unmask_checksum(mask_checksum(mask_checksum(checksum))))
        );
    }
}
