use std::ops::AddAssign;
use std::time::Duration;

/// Carries compaction metrics for a level.
pub(crate) struct LevelCompactionStats {
    /// The time it took for a compaction to complete.
    pub(crate) compaction_duration: Duration,

    /// The number of bytes read during the compaction.
    pub(crate) bytes_read: u64,

    /// The number of bytes written during the compaction.
    pub(crate) bytes_written: u64,
}

/// Private methods
impl LevelCompactionStats {
    /// Add the statistic values in `other_stats` to the statistics in this object.
    fn add_stats(&mut self, other_stats: &LevelCompactionStats) {
        self.compaction_duration += other_stats.compaction_duration;
        self.bytes_read += other_stats.bytes_read;
        self.bytes_written += other_stats.bytes_written;
    }
}

impl Default for LevelCompactionStats {
    fn default() -> Self {
        Self {
            compaction_duration: Duration::new(0, 0),
            bytes_read: 0,
            bytes_written: 0,
        }
    }
}

impl AddAssign for LevelCompactionStats {
    fn add_assign(&mut self, rhs: Self) {
        self.add_stats(&rhs);
    }
}
