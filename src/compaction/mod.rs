/*!
This module contains abstractions used in compaction operations. Core to this is the
[`CompactionWorker`] worker thread.
*/

pub(crate) mod errors;
pub(crate) use errors::CompactionWorkerError;

pub(crate) mod manifest;

pub(crate) mod manual_compaction;
pub(crate) use manual_compaction::ManualCompactionConfiguration;

pub(crate) mod state;

pub(crate) mod stats;
pub(crate) use stats::LevelCompactionStats;

pub(crate) mod utils;

pub(crate) mod worker;
pub(crate) use worker::{CompactionWorker, TaskKind};
