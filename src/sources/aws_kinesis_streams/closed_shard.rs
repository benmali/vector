//! Decision helpers for completing closed Kinesis shards after a reshard.
//!
//! After a split/merge, parent shards are CLOSED (`EndingSequenceNumber` set). These
//! helpers follow KCL 3.0: iterator choice, parent-before-child eligibility, finishing
//! only on an authoritative end-of-shard signal, and when a `SHARD_END` lease may be
//! deleted without being created again.

use std::collections::{HashMap, HashSet};

/// KCL `ExtendedSequenceNumber.SHARD_END`. Written when a shard is fully consumed
/// so children can start, then deleted once children have real checkpoints.
pub const SHARD_END: &str = "SHARD_END";

/// A shard is closed (parent after split/merge) when it has an ending sequence
/// number that is not the literal string `"null"` (some ListShards payloads).
pub fn is_shard_closed(ending_sequence_number: Option<&str>) -> bool {
    ending_sequence_number.map(|e| e != "null").unwrap_or(false)
}

pub fn is_shard_end(sequence: &str) -> bool {
    sequence == SHARD_END
}

/// Parent + adjacent-parent IDs from ListShards (`ParentShardId` / `AdjacentParentShardId`).
pub fn parent_ids(parent: Option<&str>, adjacent: Option<&str>) -> Vec<String> {
    [parent, adjacent]
        .into_iter()
        .flatten()
        .filter(|s| !s.is_empty() && *s != "null")
        .map(ToOwned::to_owned)
        .collect()
}

pub fn is_child_shard(parent_ids: &[String]) -> bool {
    !parent_ids.is_empty()
}

/// KCL `BlockOnParentShardTask`.
///
/// A parent with a non-`SHARD_END` checkpoint is still in progress. A missing row
/// for a parent that ListShards still returns is also unfinished: the parent may
/// never have been leased (for example it was split again before a consumer claimed
/// it). A missing row for a parent that is no longer listed means retention expired
/// or the lease was cleaned up, so children may start.
pub fn parents_completed(
    parent_ids: &[String],
    sequences: &HashMap<String, String>,
    listed_shard_ids: &HashSet<String>,
) -> bool {
    parent_ids.iter().all(|parent| match sequences.get(parent) {
        Some(seq) => is_shard_end(seq),
        None => !listed_shard_ids.contains(parent),
    })
}

/// Parent and closed-child links needed to tell a cleaned-up lease from a shard
/// that was never read.
pub struct ShardLineage<'a> {
    pub children_by_parent: &'a HashMap<String, Vec<String>>,
    pub closed_shard_ids: &'a HashSet<String>,
}

/// A shard's records are already accounted for when it has a real checkpoint, it
/// is no longer listed, or it is a closed shard whose own child lineages are
/// settled. An empty checkpoint is not progress. A missing row on an open shard
/// is not settled: that shard may never have been leased.
fn lineage_settled(
    shard_id: &str,
    lineage: &ShardLineage<'_>,
    sequences: &HashMap<String, String>,
    listed_shard_ids: &HashSet<String>,
    visiting: &mut HashSet<String>,
) -> bool {
    if sequences.get(shard_id).is_some_and(|seq| !seq.is_empty()) {
        return true;
    }
    if !listed_shard_ids.contains(shard_id) {
        return true;
    }
    if !lineage.closed_shard_ids.contains(shard_id) {
        return false;
    }
    if !visiting.insert(shard_id.to_string()) {
        return false;
    }
    let settled = match lineage.children_by_parent.get(shard_id) {
        Some(children) if !children.is_empty() => children
            .iter()
            .all(|child| lineage_settled(child, lineage, sequences, listed_shard_ids, visiting)),
        _ => false,
    };
    visiting.remove(shard_id);
    settled
}

/// Whether this shard may be leased this cycle (KCL 3.0 eligibility).
///
/// Never claim `SHARD_END`. Never claim a child until its parents are completed.
/// A closed shard with no row is still claimed when it is listed and its child
/// lineages are not settled, so a generation that was split before it was leased
/// is not skipped. Once every child lineage is settled, a missing parent row was
/// cleaned up after `SHARD_END` and must not be read again. A child whose own
/// `SHARD_END` row was deleted still counts as settled when its descendants have
/// real checkpoints.
pub fn should_claim_shard(
    shard_id: &str,
    has_checkpoint: bool,
    sequence: Option<&str>,
    parent_ids: &[String],
    lineage: &ShardLineage<'_>,
    sequences: &HashMap<String, String>,
    listed_shard_ids: &HashSet<String>,
) -> bool {
    if sequence.map(is_shard_end).unwrap_or(false) {
        return false;
    }
    if !parents_completed(parent_ids, sequences, listed_shard_ids) {
        return false;
    }
    let shard_closed = lineage.closed_shard_ids.contains(shard_id);
    if shard_closed && !has_checkpoint {
        let children = lineage
            .children_by_parent
            .get(shard_id)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if !children.is_empty()
            && children.iter().all(|child| {
                lineage_settled(
                    child,
                    lineage,
                    sequences,
                    listed_shard_ids,
                    &mut HashSet::new(),
                )
            })
        {
            return false;
        }
    }
    true
}

/// KCL `LeaseCleanupManager.cleanupLeaseForCompletedShard`.
///
/// Delete a `SHARD_END` row only when every child has a real sequence checkpoint
/// (not an empty just-claimed row) and this shard's own parents are already gone
/// from the table. An empty child list means children have not appeared yet.
pub fn should_delete_shard_end_lease(
    sequence: &str,
    child_ids: &[String],
    parent_ids: &[String],
    sequences: &HashMap<String, String>,
) -> bool {
    if !is_shard_end(sequence) || child_ids.is_empty() {
        return false;
    }
    if parent_ids
        .iter()
        .any(|parent| sequences.contains_key(parent))
    {
        return false;
    }
    child_ids
        .iter()
        .all(|child| sequences.get(child).is_some_and(|seq| !seq.is_empty()))
}

/// Checkpoints for shard IDs that ListShards no longer returns (retention expired).
pub fn leftover_checkpoint_shard_ids(
    checkpoint_shard_ids: impl IntoIterator<Item = String>,
    listed_shard_ids: &HashSet<String>,
) -> Vec<String> {
    checkpoint_shard_ids
        .into_iter()
        .filter(|id| !listed_shard_ids.contains(id))
        .collect()
}

/// Map each parent shard ID to the child shard IDs that name it as a parent.
pub fn children_by_parent(shards: &[(String, Vec<String>)]) -> HashMap<String, Vec<String>> {
    let mut map: HashMap<String, Vec<String>> = HashMap::new();
    for (shard_id, parents) in shards {
        for parent in parents {
            map.entry(parent.clone())
                .or_default()
                .push(shard_id.clone());
        }
    }
    map
}

/// Which `GetShardIterator` type to request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardIteratorChoice {
    AfterSequenceNumber,
    TrimHorizon,
    Latest,
}

/// Choose a shard iterator type.
///
/// Closed shards must never use `LATEST`: that parks the iterator at the last
/// record and iterator age grows until retention. Empty sequence on a closed
/// shard uses `TRIM_HORIZON` so remaining records are drained.
///
/// Child shards also use `TRIM_HORIZON` when they have no sequence, matching
/// KCL 3.0 (initial position on a child is TRIM_HORIZON even if the configured
/// position is LATEST, so records are not skipped after a reshard).
pub fn shard_iterator_choice(
    sequence: &str,
    start_from_oldest: bool,
    shard_closed: bool,
    is_child: bool,
) -> ShardIteratorChoice {
    if !sequence.is_empty() && !is_shard_end(sequence) {
        ShardIteratorChoice::AfterSequenceNumber
    } else if shard_closed || is_child || start_from_oldest {
        ShardIteratorChoice::TrimHorizon
    } else {
        ShardIteratorChoice::Latest
    }
}

/// Whether the per-shard consumer should stop and checkpoint `SHARD_END`.
///
/// AWS ends a shard only when `NextShardIterator` is null, or when `ChildShards`
/// is present (returned only once the end of the shard has been reached). An empty
/// `GetRecords` page still has later records, and a rising `MillisBehindLatest`
/// means the consumer is behind, not that the shard is closed.
pub fn should_finish_shard_consumer(
    next_iterator_missing: bool,
    child_shards_present: bool,
) -> bool {
    next_iterator_missing || child_shards_present
}

/// Action after a failed `GetShardIterator`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IteratorErrorAction {
    /// Shard is gone (`ResourceNotFound`); delete the DynamoDB row.
    Delete,
    /// Transient or non-terminal failure. Keep the sequence and retry on the next claim.
    Release,
}

pub fn iterator_error_action(resource_not_found: bool) -> IteratorErrorAction {
    if resource_not_found {
        IteratorErrorAction::Delete
    } else {
        IteratorErrorAction::Release
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};

    #[test]
    fn open_shard_has_no_ending_sequence() {
        assert!(!is_shard_closed(None));
    }

    #[test]
    fn closed_shard_has_ending_sequence() {
        assert!(is_shard_closed(Some(
            "49671246667589458717803282320587893555896035326582658"
        )));
    }

    #[test]
    fn ending_sequence_null_string_is_open() {
        assert!(!is_shard_closed(Some("null")));
    }

    #[test]
    fn iterator_after_sequence_when_checkpoint_exists() {
        assert_eq!(
            shard_iterator_choice("4967", false, true, false),
            ShardIteratorChoice::AfterSequenceNumber
        );
        assert_eq!(
            shard_iterator_choice("4967", false, false, true),
            ShardIteratorChoice::AfterSequenceNumber
        );
    }

    #[test]
    fn iterator_never_latest_on_closed_shard() {
        assert_eq!(
            shard_iterator_choice("", false, true, false),
            ShardIteratorChoice::TrimHorizon
        );
        assert_eq!(
            shard_iterator_choice("", true, true, false),
            ShardIteratorChoice::TrimHorizon
        );
    }

    #[test]
    fn iterator_trim_horizon_on_child_even_when_not_start_from_oldest() {
        assert_eq!(
            shard_iterator_choice("", false, false, true),
            ShardIteratorChoice::TrimHorizon
        );
    }

    #[test]
    fn iterator_latest_on_open_root_shard_when_not_start_from_oldest() {
        assert_eq!(
            shard_iterator_choice("", false, false, false),
            ShardIteratorChoice::Latest
        );
    }

    #[test]
    fn iterator_trim_horizon_on_open_shard_when_start_from_oldest() {
        assert_eq!(
            shard_iterator_choice("", true, false, false),
            ShardIteratorChoice::TrimHorizon
        );
    }

    #[test]
    fn finish_when_next_iterator_missing() {
        assert!(should_finish_shard_consumer(true, false));
    }

    #[test]
    fn finish_when_child_shards_present() {
        assert!(should_finish_shard_consumer(false, true));
    }

    #[test]
    fn do_not_finish_on_empty_page_with_next_iterator() {
        assert!(!should_finish_shard_consumer(false, false));
    }

    #[test]
    fn iterator_error_deletes_only_when_shard_is_gone() {
        assert_eq!(iterator_error_action(true), IteratorErrorAction::Delete);
        assert_eq!(iterator_error_action(false), IteratorErrorAction::Release);
    }

    #[test]
    fn parent_ids_skip_empty_and_null() {
        assert_eq!(
            parent_ids(Some("shard-parent"), Some("shard-adjacent")),
            vec!["shard-parent", "shard-adjacent"]
        );
        assert!(parent_ids(None, None).is_empty());
        assert!(parent_ids(Some(""), Some("null")).is_empty());
        assert_eq!(parent_ids(Some("shard-parent"), None), vec!["shard-parent"]);
    }

    fn listed(ids: &[&str]) -> HashSet<String> {
        ids.iter().map(|id| (*id).to_string()).collect()
    }

    fn lineage<'a>(
        children_by_parent: &'a HashMap<String, Vec<String>>,
        closed_shard_ids: &'a HashSet<String>,
    ) -> ShardLineage<'a> {
        ShardLineage {
            children_by_parent,
            closed_shard_ids,
        }
    }

    #[test]
    fn parents_completed_when_shard_end_or_unlisted() {
        let mut sequences = HashMap::new();
        sequences.insert("p1".into(), SHARD_END.into());
        let listed_p1_p2 = listed(&["p1", "p2"]);
        assert!(!parents_completed(
            &["p1".into(), "p2".into()],
            &sequences,
            &listed_p1_p2,
        ));
        sequences.insert("p2".into(), SHARD_END.into());
        assert!(parents_completed(
            &["p1".into(), "p2".into()],
            &sequences,
            &listed_p1_p2,
        ));
        assert!(parents_completed(&[], &sequences, &listed_p1_p2));
        sequences.remove("p2");
        let listed_p1 = listed(&["p1"]);
        assert!(parents_completed(
            &["p1".into(), "p2".into()],
            &sequences,
            &listed_p1,
        ));
    }

    #[test]
    fn listed_parent_without_checkpoint_blocks_children() {
        let sequences = HashMap::new();
        let listed = listed(&["parent"]);
        assert!(!parents_completed(&["parent".into()], &sequences, &listed,));
        assert!(parents_completed(
            &["parent".into()],
            &sequences,
            &HashSet::new(),
        ));
    }

    #[test]
    fn do_not_claim_shard_end_or_blocked_child() {
        let mut sequences = HashMap::new();
        sequences.insert("parent".into(), "4967".into());
        sequences.insert("done-parent".into(), SHARD_END.into());
        let listed_ids = listed(&["parent", "done-parent", "trimmed-parent", "closed"]);
        let children = HashMap::new();
        let closed_ids = listed(&["closed"]);
        let lineage = lineage(&children, &closed_ids);

        assert!(!should_claim_shard(
            "closed",
            true,
            Some(SHARD_END),
            &[],
            &lineage,
            &sequences,
            &listed_ids,
        ));
        assert!(!should_claim_shard(
            "child",
            false,
            None,
            &["parent".into()],
            &lineage,
            &sequences,
            &listed_ids,
        ));
        assert!(should_claim_shard(
            "child",
            false,
            None,
            &["done-parent".into()],
            &lineage,
            &sequences,
            &listed_ids,
        ));
        assert!(!should_claim_shard(
            "child",
            false,
            None,
            &["trimmed-parent".into()],
            &lineage,
            &sequences,
            &listed_ids,
        ));
        assert!(should_claim_shard(
            "closed",
            false,
            None,
            &[],
            &lineage,
            &sequences,
            &listed_ids,
        ));
        assert!(should_claim_shard(
            "closed",
            true,
            Some(""),
            &[],
            &lineage,
            &sequences,
            &listed_ids,
        ));
    }

    #[test]
    fn claim_unleased_closed_shard_until_children_have_checkpoints() {
        let mut sequences = HashMap::new();
        let listed_ids = listed(&["closed-parent", "child-a", "child-b"]);
        let children = children_by_parent(&[
            ("child-a".into(), vec!["closed-parent".into()]),
            ("child-b".into(), vec!["closed-parent".into()]),
        ]);
        let closed_ids = listed(&["closed-parent"]);
        let lineage = lineage(&children, &closed_ids);
        assert!(should_claim_shard(
            "closed-parent",
            false,
            None,
            &[],
            &lineage,
            &sequences,
            &listed_ids,
        ));
        sequences.insert("child-a".into(), "1".into());
        sequences.insert("child-b".into(), "2".into());
        assert!(!should_claim_shard(
            "closed-parent",
            false,
            None,
            &[],
            &lineage,
            &sequences,
            &listed_ids,
        ));
    }

    #[test]
    fn do_not_reclaim_parent_after_intermediate_child_lease_is_deleted() {
        // 000 was consumed and its SHARD_END row deleted. 001 was then split,
        // consumed, and its SHARD_END row deleted too. 002 and the grandchildren
        // still have real checkpoints, so 000 must not be read again.
        let mut sequences = HashMap::new();
        sequences.insert("002".into(), "20".into());
        sequences.insert("003".into(), "30".into());
        sequences.insert("004".into(), "40".into());
        let listed_ids = listed(&["000", "001", "002", "003", "004"]);
        let children = children_by_parent(&[
            ("001".into(), vec!["000".into()]),
            ("002".into(), vec!["000".into()]),
            ("003".into(), vec!["001".into()]),
            ("004".into(), vec!["001".into()]),
        ]);
        let closed_ids = listed(&["000", "001"]);
        let lineage = lineage(&children, &closed_ids);
        assert!(!should_claim_shard(
            "000",
            false,
            None,
            &[],
            &lineage,
            &sequences,
            &listed_ids,
        ));

        // The same tree with no checkpoints anywhere is an unleased generation.
        let sequences = HashMap::new();
        assert!(should_claim_shard(
            "000",
            false,
            None,
            &[],
            &lineage,
            &sequences,
            &listed_ids,
        ));
    }

    #[test]
    fn delete_shard_end_only_after_children_have_real_checkpoints() {
        let mut sequences = HashMap::new();
        sequences.insert("parent".into(), SHARD_END.into());
        assert!(!should_delete_shard_end_lease(
            SHARD_END,
            &["child-a".into(), "child-b".into()],
            &[],
            &sequences
        ));
        sequences.insert("child-a".into(), "1".into());
        sequences.insert("child-b".into(), "".into());
        assert!(!should_delete_shard_end_lease(
            SHARD_END,
            &["child-a".into(), "child-b".into()],
            &[],
            &sequences
        ));
        sequences.insert("child-b".into(), "2".into());
        assert!(should_delete_shard_end_lease(
            SHARD_END,
            &["child-a".into(), "child-b".into()],
            &[],
            &sequences
        ));
        sequences.insert("grandparent".into(), SHARD_END.into());
        assert!(!should_delete_shard_end_lease(
            SHARD_END,
            &["child-a".into(), "child-b".into()],
            &["grandparent".into()],
            &sequences
        ));
        assert!(!should_delete_shard_end_lease(
            SHARD_END,
            &[],
            &[],
            &sequences
        ));
        assert!(!should_delete_shard_end_lease(
            "4967",
            &["child-a".into()],
            &[],
            &sequences
        ));
    }

    #[test]
    fn leftover_ids_are_checkpoints_missing_from_list_shards() {
        let listed = HashSet::from(["open-1".into(), "open-2".into()]);
        let leftover = leftover_checkpoint_shard_ids(
            vec![
                "open-1".into(),
                "expired-parent".into(),
                "open-2".into(),
                "expired-empty".into(),
            ],
            &listed,
        );
        assert_eq!(leftover, vec!["expired-parent", "expired-empty"]);
    }

    #[test]
    fn children_map_from_parent_pointers() {
        let shards = vec![
            ("child-a".into(), vec!["parent".into()]),
            ("child-b".into(), vec!["parent".into()]),
            ("merged".into(), vec!["p1".into(), "p2".into()]),
        ];
        let map = children_by_parent(&shards);
        assert_eq!(map.get("parent").unwrap().len(), 2);
        assert_eq!(map.get("p1").unwrap(), &vec!["merged".to_string()]);
        assert_eq!(map.get("p2").unwrap(), &vec!["merged".to_string()]);
    }
}
