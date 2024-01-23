// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{
    fmt,
    hash::{Hash, Hasher},
    ops::Deref,
    sync::Arc,
    time::SystemTime,
};

use bytes::Bytes;
use consensus_config::{AuthorityIndex, DefaultHashFunction, Epoch, DIGEST_LENGTH};
use enum_dispatch::enum_dispatch;
use fastcrypto::hash::{Digest, HashFunction};
use serde::{Deserialize, Serialize};

use crate::context::Context;

const GENESIS_ROUND: Round = 0;

/// Round number of a block.
pub type Round = u32;

/// Block proposal timestamp in milliseconds.
pub type BlockTimestampMs = u64;

// Returns the current time expressed as UNIX timestamp in milliseconds.
pub fn timestamp_utc_ms() -> BlockTimestampMs {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_millis() as BlockTimestampMs,
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

/// The transaction serialised bytes
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Default, Debug)]
pub struct Transaction {
    data: Bytes,
}

#[allow(dead_code)]
impl Transaction {
    pub fn new(data: Vec<u8>) -> Self {
        Self { data: data.into() }
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn into_data(self) -> Bytes {
        self.data
    }
}

/// A block includes references to previous round blocks and transactions that the authority
/// considers valid.
/// Well behaved authorities produce at most one block per round, but malicious authorities can
/// equivocate.
#[derive(Clone, Deserialize, Serialize)]
#[enum_dispatch(BlockAPI)]
pub enum Block {
    V1(BlockV1),
}

impl Block {
    /// Generate the genesis blocks for the latest Block version. The tuple contains (my_genesis_block, others_genesis_blocks).
    /// The blocks are returned in authority index order.
    pub(crate) fn genesis(context: Arc<Context>) -> (VerifiedBlock, Vec<VerifiedBlock>) {
        let (my_block, others_block): (Vec<_>, Vec<_>) = context
            .committee
            .authorities()
            .map(|(authority_index, _)| {
                let signed = SignedBlock::new(Block::V1(BlockV1::genesis(
                    authority_index,
                    context.committee.epoch(),
                )));
                VerifiedBlock::new_verified_unserialized(signed)
                    .expect("Shouldn't fail when creating verified block for genesis")
            })
            .partition(|block| block.author() == context.own_index);
        (my_block[0].clone(), others_block)
    }
}

#[enum_dispatch]
pub trait BlockAPI {
    fn epoch(&self) -> Epoch;
    fn round(&self) -> Round;
    fn author(&self) -> AuthorityIndex;
    fn timestamp_ms(&self) -> BlockTimestampMs;
    fn ancestors(&self) -> &[BlockRef];
    fn transactions(&self) -> &[Transaction];
}

#[derive(Clone, Default, Deserialize, Serialize)]
pub struct BlockV1 {
    epoch: Epoch,
    round: Round,
    author: AuthorityIndex,
    // TODO: during verification ensure that timestamp_ms >= ancestors.timestamp
    timestamp_ms: BlockTimestampMs,
    ancestors: Vec<BlockRef>,
    transactions: Vec<Transaction>,
}

impl BlockV1 {
    #[allow(dead_code)]
    pub(crate) fn new(
        epoch: Epoch,
        round: Round,
        author: AuthorityIndex,
        timestamp_ms: BlockTimestampMs,
        ancestors: Vec<BlockRef>,
        transactions: Vec<Transaction>,
    ) -> BlockV1 {
        Self {
            round,
            author,
            timestamp_ms,
            ancestors,
            transactions,
            epoch,
        }
    }

    /// Generate the block that is meant to be used for genesis
    pub(crate) fn genesis(author: AuthorityIndex, epoch: Epoch) -> BlockV1 {
        Self {
            round: GENESIS_ROUND,
            author,
            timestamp_ms: 0,
            ancestors: vec![],
            transactions: vec![],
            epoch,
        }
    }
}

impl BlockAPI for BlockV1 {
    fn round(&self) -> Round {
        self.round
    }

    fn author(&self) -> AuthorityIndex {
        self.author
    }

    fn timestamp_ms(&self) -> BlockTimestampMs {
        self.timestamp_ms
    }

    fn ancestors(&self) -> &[BlockRef] {
        &self.ancestors
    }

    fn transactions(&self) -> &[Transaction] {
        &self.transactions
    }

    fn epoch(&self) -> Epoch {
        self.epoch
    }
}

/// BlockRef is the minimum info that uniquely identify a block.
#[derive(Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockRef {
    pub round: Round,
    pub author: AuthorityIndex,
    pub digest: BlockDigest,
}

#[allow(unused)]
impl BlockRef {
    pub fn new(round: Round, author: AuthorityIndex, digest: BlockDigest) -> Self {
        Self {
            round,
            author,
            digest,
        }
    }
}

impl fmt::Display for BlockRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}({};{})", self.round, self.author, self.digest)
    }
}

impl fmt::Debug for BlockRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}({};{:?})", self.round, self.author, self.digest)
    }
}

impl Hash for BlockRef {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self.digest.0[..8]);
    }
}

/// Hash of a block, covers all fields except signature.
#[derive(Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct BlockDigest([u8; consensus_config::DIGEST_LENGTH]);

impl BlockDigest {
    /// Lexicographic min & max digest.
    pub const MIN: Self = Self([u8::MIN; consensus_config::DIGEST_LENGTH]);
    pub const MAX: Self = Self([u8::MAX; consensus_config::DIGEST_LENGTH]);
}

impl Hash for BlockDigest {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write(&self.0[..8]);
    }
}

impl From<BlockDigest> for Digest<{ DIGEST_LENGTH }> {
    fn from(hd: BlockDigest) -> Self {
        Digest::new(hd.0)
    }
}

impl fmt::Display for BlockDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}",
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, self.0)
                .get(0..4)
                .ok_or(fmt::Error)?
        )
    }
}

impl fmt::Debug for BlockDigest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}",
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, self.0)
        )
    }
}

impl AsRef<[u8]> for BlockDigest {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Slot is the position of blocks in the DAG. It can contain 0, 1 or multiple blocks
/// from the same authority at the same round.
#[derive(Clone, Copy, PartialEq, PartialOrd, Default, Hash)]
pub(crate) struct Slot {
    pub round: Round,
    pub authority: AuthorityIndex,
}

impl Slot {
    pub fn new(round: Round, authority: AuthorityIndex) -> Self {
        Self { round, authority }
    }
}

impl From<BlockRef> for Slot {
    fn from(value: BlockRef) -> Self {
        Slot::new(value.round, value.author)
    }
}

impl fmt::Display for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.round, self.authority)
    }
}

impl fmt::Debug for Slot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// Unverified block only allows limited access to its content.
#[allow(unused)]
#[derive(Deserialize, Serialize)]
pub(crate) struct SignedBlock {
    inner: Block,
    signature: Bytes,
}

impl SignedBlock {
    // TODO: add verification.

    // TODO: will refactor once the signing approach has been introduced.
    pub(crate) fn new(block: Block) -> Self {
        Self {
            inner: block,
            signature: Bytes::default(),
        }
    }

    /// Serialises the block using the bcs serializer
    pub(crate) fn serialize(&self) -> Result<Bytes, bcs::Error> {
        let bytes = bcs::to_bytes(self)?;
        Ok(bytes.into())
    }
}

/// VerifiedBlock allows full access to its content.
/// It should be relatively cheap to copy.
#[derive(Clone)]
pub(crate) struct VerifiedBlock {
    block: Arc<SignedBlock>,

    // Cached Block digest and serialized SignedBlock, to avoid re-computing these values.
    digest: BlockDigest,
    serialized: Bytes,
}

impl VerifiedBlock {
    /// Creates VerifiedBlock from verified SignedBlock and its serialized bytes.
    pub fn new_verified(signed_block: SignedBlock, serialized: Bytes) -> Result<Self, bcs::Error> {
        let digest = Self::compute_digest(&signed_block.inner)?;
        Ok(VerifiedBlock {
            block: Arc::new(signed_block),
            digest,
            serialized,
        })
    }

    /// Creates a new VerifiedBlock from a SignedBlock and the serialized bytes aren't available. Primarily this should be
    /// used when proposing a new block and the bytes aren't available.
    pub fn new_verified_unserialized(signed_block: SignedBlock) -> Result<Self, bcs::Error> {
        let serialized = signed_block.serialize()?;
        Self::new_verified(signed_block, serialized)
    }

    #[cfg(test)]
    pub(crate) fn new_for_test(block: Block) -> Self {
        let digest = Self::compute_digest(&block).unwrap();
        // Use empty signature in test.
        let signed_block = SignedBlock {
            inner: block,
            signature: Default::default(),
        };
        let serialized: Bytes = bcs::to_bytes(&signed_block)
            .expect("Serialization should not fail")
            .into();
        VerifiedBlock {
            block: Arc::new(signed_block),
            digest,
            serialized,
        }
    }

    /// Returns reference to the block.
    pub fn reference(&self) -> BlockRef {
        BlockRef {
            round: self.round(),
            author: self.author(),
            digest: self.digest(),
        }
    }

    pub fn digest(&self) -> BlockDigest {
        self.digest
    }

    /// Returns the serialized block with signature.
    pub fn serialized(&self) -> &Bytes {
        &self.serialized
    }

    fn compute_digest(block: &Block) -> Result<BlockDigest, bcs::Error> {
        let mut hasher = DefaultHashFunction::new();
        hasher.update(bcs::to_bytes(block)?);
        Ok(BlockDigest(hasher.finalize().into()))
    }
}

/// Allow quick access on the underlying Block without having to always refer to the inner block ref.
impl Deref for VerifiedBlock {
    type Target = Block;

    fn deref(&self) -> &Self::Target {
        &self.block.inner
    }
}

impl PartialEq for VerifiedBlock {
    fn eq(&self, other: &Self) -> bool {
        self.digest() == other.digest()
    }
}

impl fmt::Display for VerifiedBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(f, "{}", self.reference())
    }
}

impl fmt::Debug for VerifiedBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
        write!(
            f,
            "{:?}({};{:?};v)",
            self.reference(),
            self.timestamp_ms(),
            self.ancestors()
        )
    }
}

/// Creates fake blocks for testing.
#[cfg(test)]
pub(crate) struct TestBlock {
    block: BlockV1,
}

#[allow(unused)]
#[cfg(test)]
impl TestBlock {
    pub(crate) fn new(round: Round, author: u32) -> Self {
        Self {
            block: BlockV1 {
                round,
                author: AuthorityIndex::new_for_test(author),
                ..Default::default()
            },
        }
    }

    pub(crate) fn set_round(mut self, round: Round) -> Self {
        self.block.round = round;
        self
    }

    pub(crate) fn set_author(mut self, author: AuthorityIndex) -> Self {
        self.block.author = author;
        self
    }

    pub(crate) fn set_timestamp_ms(mut self, timestamp_ms: BlockTimestampMs) -> Self {
        self.block.timestamp_ms = timestamp_ms;
        self
    }

    pub(crate) fn set_ancestors(mut self, ancestors: Vec<BlockRef>) -> Self {
        self.block.ancestors = ancestors;
        self
    }

    pub(crate) fn set_epoch(mut self, epoch: Epoch) -> Self {
        self.block.epoch = epoch;
        self
    }

    pub(crate) fn build(self) -> Block {
        Block::V1(self.block)
    }
}

// TODO: add basic verification for BlockRef and BlockDigest.
// TODO: add tests for SignedBlock and VerifiedBlock conversion.
