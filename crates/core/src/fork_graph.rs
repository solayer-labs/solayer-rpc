use solana_program_runtime::loaded_programs::{BlockRelation, ForkGraph};
use solana_sdk::clock::Slot;

#[derive(Default)]
pub struct EmptyForkGraph;

impl ForkGraph for EmptyForkGraph {
    fn relationship(&self, a: Slot, b: Slot) -> BlockRelation {
        match a.cmp(&b) {
            std::cmp::Ordering::Less => BlockRelation::Ancestor,
            std::cmp::Ordering::Equal => BlockRelation::Equal,
            std::cmp::Ordering::Greater => BlockRelation::Descendant,
        }
    }
}
