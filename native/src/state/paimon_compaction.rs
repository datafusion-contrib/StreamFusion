//! Java Paimon's universal compaction pick strategy, ported — not reinvented — for the state
//! store's checkpoint-time maintenance (paimon-rust has no compaction manager yet; this is the
//! decision half of `org.apache.paimon.mergetree.compact.UniversalCompaction`, RocksDB
//! universal style, upstream-contribution material).
//!
//! Semantics preserved from Java: files organize into *sorted runs* — every level-0 file is its
//! own run (newest first, by max sequence number), every non-empty level ≥ 1 is one run — and the
//! list is ordered newest → oldest. A pick is always a newest-prefix of that list, chosen by, in
//! order: space amplification (all runs vs the oldest run's size), size ratio (grow the candidate
//! while the next run is not disproportionately larger), and the sorted-run count trigger. The
//! output level is one below the first excluded run (never 0), or the maximum level when the pick
//! covers everything — only then may deletions be dropped, because nothing older remains.
//!
//! The newest-prefix property is also this store's *correctness* condition, not just policy: the
//! rewrite path assigns fresh sequence numbers to the merged rows (Java's rewriter preserves
//! per-record sequences; paimon-rust's write path cannot yet), which is sound exactly when every
//! run excluded from the pick is older than every run included.

use paimon::spec::DataFileMeta;

/// Java defaults: `compaction.max-size-amplification-percent` and `compaction.size-ratio`.
pub(crate) const MAX_SIZE_AMPLIFICATION_PERCENT: i64 = 200;
pub(crate) const SIZE_RATIO_PERCENT: i64 = 1;

/// One sorted run: a level-0 file by itself, or all of one higher level's files.
pub(crate) struct LevelSortedRun {
    pub level: i32,
    pub files: Vec<DataFileMeta>,
    pub total_size: i64,
}

/// A picked compaction: merge `files` (a newest-prefix of the runs) into `output_level`.
pub(crate) struct CompactUnit {
    pub output_level: i32,
    pub files: Vec<DataFileMeta>,
    /// The pick covers every run — the merge output is the oldest data, so deletions may drop.
    pub full: bool,
}

/// Organizes a bucket's live files into the newest→oldest run list (Java `Levels`): level-0 files
/// each stand alone ordered by max sequence descending (min sequence ascending on ties), then one
/// run per non-empty level ascending.
pub(crate) fn level_sorted_runs(files: &[DataFileMeta]) -> Vec<LevelSortedRun> {
    let mut level0: Vec<&DataFileMeta> = files.iter().filter(|f| f.level == 0).collect();
    level0.sort_by(|a, b| {
        b.max_sequence_number
            .cmp(&a.max_sequence_number)
            .then(a.min_sequence_number.cmp(&b.min_sequence_number))
            .then(a.file_name.cmp(&b.file_name))
    });
    let mut runs: Vec<LevelSortedRun> = level0
        .into_iter()
        .map(|f| LevelSortedRun {
            level: 0,
            files: vec![f.clone()],
            total_size: f.file_size,
        })
        .collect();
    let mut max_level = 0;
    for file in files {
        max_level = max_level.max(file.level);
    }
    for level in 1..=max_level {
        let level_files: Vec<DataFileMeta> =
            files.iter().filter(|f| f.level == level).cloned().collect();
        if !level_files.is_empty() {
            let total_size = level_files.iter().map(|f| f.file_size).sum();
            runs.push(LevelSortedRun { level, files: level_files, total_size });
        }
    }
    runs
}

/// `UniversalCompaction.pick`: `num_run_trigger` is Paimon's
/// `num-sorted-run.compaction-trigger`; `num_levels` follows Java's default of trigger + 1.
pub(crate) fn pick(num_run_trigger: usize, runs: &[LevelSortedRun]) -> Option<CompactUnit> {
    let num_levels = (num_run_trigger + 1).max(2) as i32;
    let max_level = num_levels - 1;

    // 1 checking for reducing size amplification
    if let Some(unit) = pick_for_size_amp(num_run_trigger, max_level, runs) {
        return Some(unit);
    }
    // 2 checking for size ratio
    if runs.len() >= num_run_trigger {
        if let Some(unit) = pick_for_size_ratio(max_level, runs, 1) {
            return Some(unit);
        }
    }
    // 3 checking for run count
    if runs.len() > num_run_trigger {
        let candidate_count = runs.len() - num_run_trigger + 1;
        return pick_for_size_ratio(max_level, runs, candidate_count);
    }
    None
}

fn pick_for_size_amp(
    num_run_trigger: usize,
    max_level: i32,
    runs: &[LevelSortedRun],
) -> Option<CompactUnit> {
    if runs.len() < num_run_trigger {
        return None;
    }
    let candidate_size: i64 = runs[..runs.len() - 1].iter().map(|r| r.total_size).sum();
    let earliest_run_size = runs[runs.len() - 1].total_size;
    // size amplification = percentage of additional size
    if candidate_size * 100 > MAX_SIZE_AMPLIFICATION_PERCENT * earliest_run_size {
        return Some(unit_of(runs, max_level, runs.len()));
    }
    None
}

fn pick_for_size_ratio(
    max_level: i32,
    runs: &[LevelSortedRun],
    mut candidate_count: usize,
) -> Option<CompactUnit> {
    let mut candidate_size: i64 = runs[..candidate_count].iter().map(|r| r.total_size).sum();
    for next in &runs[candidate_count..] {
        if (candidate_size as f64) * ((100 + SIZE_RATIO_PERCENT) as f64) / 100.0
            < next.total_size as f64
        {
            break;
        }
        candidate_size += next.total_size;
        candidate_count += 1;
    }
    if candidate_count > 1 {
        return Some(unit_of(runs, max_level, candidate_count));
    }
    None
}

/// `UniversalCompaction.createUnit`: output one level below the first excluded run, never level 0
/// (swallowing further runs if needed), the maximum level when everything is included.
fn unit_of(runs: &[LevelSortedRun], max_level: i32, mut run_count: usize) -> CompactUnit {
    let mut output_level;
    if run_count == runs.len() {
        output_level = max_level;
    } else {
        output_level = (runs[run_count].level - 1).max(0);
    }
    if output_level == 0 {
        // do not output level 0
        for next in &runs[run_count..] {
            run_count += 1;
            if next.level != 0 {
                output_level = next.level;
                break;
            }
        }
    }
    let full = run_count == runs.len();
    if full {
        output_level = max_level;
    }
    CompactUnit {
        output_level,
        files: runs[..run_count].iter().flat_map(|r| r.files.iter().cloned()).collect(),
        full,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a meta through its serde form: the stats type the struct embeds is crate-private
    /// in paimon-rust, so a literal cannot name it.
    fn file(name: &str, level: i32, size: i64, max_seq: i64) -> DataFileMeta {
        let empty_stats =
            serde_json::json!({ "_MIN_VALUES": [], "_MAX_VALUES": [], "_NULL_COUNTS": null });
        serde_json::from_value(serde_json::json!({
            "_FILE_NAME": name,
            "_FILE_SIZE": size,
            "_ROW_COUNT": size,
            "_MIN_KEY": [],
            "_MAX_KEY": [],
            "_KEY_STATS": empty_stats,
            "_VALUE_STATS": empty_stats,
            "_MIN_SEQUENCE_NUMBER": max_seq,
            "_MAX_SEQUENCE_NUMBER": max_seq,
            "_SCHEMA_ID": 0,
            "_LEVEL": level,
            "_EXTRA_FILES": [],
            "_DELETE_ROW_COUNT": 0,
            "_EMBEDDED_FILE_INDEX": null,
        }))
        .expect("test data file meta")
    }

    #[test]
    fn a_compacted_base_alone_never_repicks() {
        // The pathology a plain file-count trigger has: a big base rolled into many files is one
        // run, and one run is never compacted again, no matter how many files it spans.
        let base: Vec<DataFileMeta> =
            (0..10).map(|i| file(&format!("b{i}"), 1, 128, i)).collect();
        let runs = level_sorted_runs(&base);
        assert_eq!(runs.len(), 1);
        assert!(pick(2, &runs).is_none());
    }

    #[test]
    fn run_count_trigger_merges_the_newest_prefix() {
        // Small fresh L0 runs on a big base: the run-count trigger merges the L0s together but
        // leaves the disproportionately larger base alone (size ratio breaks the extension).
        let mut files = vec![file("base", 2, 100_000, 0)];
        for i in 0..3 {
            files.push(file(&format!("l0-{i}"), 0, 10, 10 + i as i64));
        }
        let runs = level_sorted_runs(&files);
        assert_eq!(runs.len(), 4);
        let unit = pick(3, &runs).expect("over trigger");
        assert!(!unit.full);
        assert_eq!(unit.files.len(), 3, "the three L0 runs, not the base");
        assert!(unit.files.iter().all(|f| f.level == 0));
        assert_eq!(unit.output_level, 1, "one below the base's level");
    }

    #[test]
    fn size_amplification_forces_a_full_merge() {
        // Accumulated newer data much larger than the oldest run → full compaction to max level.
        let files = vec![
            file("old", 1, 10, 0),
            file("n1", 0, 100, 1),
            file("n2", 0, 100, 2),
        ];
        let runs = level_sorted_runs(&files);
        let unit = pick(3, &runs).expect("size amp");
        assert!(unit.full);
        assert_eq!(unit.files.len(), 3);
        assert_eq!(unit.output_level, 3); // num_levels = trigger + 1 = 4, max level 3
    }

    #[test]
    fn similar_sized_runs_merge_by_size_ratio() {
        let files: Vec<DataFileMeta> =
            (0..5).map(|i| file(&format!("f{i}"), 0, 100, i)).collect();
        let unit = pick(5, &level_sorted_runs(&files)).expect("at trigger, ratio merges");
        assert!(unit.full, "five equal runs all fold into one");
    }

    #[test]
    fn below_trigger_no_pick() {
        let files = vec![file("a", 0, 100, 1), file("b", 1, 100, 0)];
        assert!(pick(5, &level_sorted_runs(&files)).is_none());
    }
}
