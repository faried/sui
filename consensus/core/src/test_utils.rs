use std::sync::Arc;

use consensus_config::{AuthorityIndex, Committee, Parameters};
use parking_lot::RwLock;
use sui_protocol_config::ProtocolConfig;

use crate::{
    block::{BlockRef, Round, TestBlock, VerifiedBlock},
    context::Context,
    dag_state::DagState,
    metrics::test_metrics,
};

/// A helper function to create a test context with a committee of given size
/// and even stake
pub fn context_for_test(committee_size: usize) -> Arc<Context> {
    let committee = Committee::new_for_test(0, vec![1; committee_size]).0;
    let metrics = test_metrics();
    Arc::new(Context::new(
        AuthorityIndex::new_for_test(0),
        committee,
        Parameters::default(),
        ProtocolConfig::get_for_min_version(),
        metrics,
    ))
}

/// Build a fully interconnected dag up to the specified round. This function
/// starts building the dag from the specified [`start`] references or from
/// genesis if none are specified. The round [`stop``] references is not inclusive
/// to account for the rounds being zero indexed.
pub fn build_dag(
    context: Arc<Context>,
    dag_state: Arc<RwLock<DagState>>,
    start: Option<Vec<BlockRef>>,
    stop: Round,
) -> Vec<BlockRef> {
    let mut ancestors = match start {
        Some(start) => {
            assert!(!start.is_empty());
            assert_eq!(
                start.iter().map(|x| x.round).max(),
                start.iter().map(|x| x.round).min()
            );
            start
        }
        None => {
            let (references, genesis): (Vec<_>, Vec<_>) = context
                .committee
                .authorities()
                .map(|index| {
                    let author_idx = index.0.value() as u32;
                    let block = TestBlock::new(0, author_idx).build();
                    VerifiedBlock::new_for_test(block)
                })
                .map(|block| (block.reference(), block))
                .unzip();
            dag_state.write().accept_blocks(genesis);

            references
        }
    };

    let starting_round = ancestors.first().unwrap().round + 1;
    for round in starting_round..stop {
        let (references, blocks): (Vec<_>, Vec<_>) = context
            .committee
            .authorities()
            .map(|authority| {
                let author_idx = authority.0.value() as u32;
                let block = VerifiedBlock::new_for_test(
                    TestBlock::new(round, author_idx)
                        .set_ancestors(ancestors.clone())
                        .build(),
                );

                (block.reference(), block)
            })
            .unzip();
        dag_state.write().accept_blocks(blocks);
        ancestors = references;
    }

    ancestors
}

pub fn build_dag_layer(
    // A list of (authority, parents) pairs. For each authority, we add a block
    // linking to the specified parents.
    connections: Vec<(AuthorityIndex, Vec<BlockRef>)>,
    dag_state: Arc<RwLock<DagState>>,
) -> Vec<BlockRef> {
    let mut references = Vec::new();
    for (authority, ancestors) in connections {
        let round = ancestors.first().unwrap().round + 1;
        let author = authority.value() as u32;
        let block = VerifiedBlock::new_for_test(
            TestBlock::new(round, author)
                .set_ancestors(ancestors)
                .build(),
        );
        references.push(block.reference());
        dag_state.write().accept_block(block);
    }
    references
}
