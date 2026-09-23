use alloy_consensus::{constants::KECCAK_EMPTY, BlockHeader};
use alloy_primitives::{hex, Address, B256};
use base_common_consensus::BasePrimitives;
use base_execution_chainspec::BaseChainSpec;
use base_execution_consensus::BaseBeaconConsensus;
use base_execution_evm::{BaseEvmConfig, BaseRethReceiptBuilder};
use base_node_core::BaseNode;
use reth_consensus::FullConsensus;
use reth_evm::{execute::Executor, ConfigureEvm};
use reth_provider::{providers::ProviderFactoryBuilder, BlockReader, TransactionVariant};
use reth_revm::database::StateProviderDatabase;
use reth_tasks::{RuntimeBuilder, RuntimeConfig};
use std::{collections::{BTreeMap, BTreeSet}, env, sync::Arc};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 4 {
        return Err("usage: fossil-state-probe <datadir> <first> <last>".into());
    }
    let first: u64 = args[2].parse()?;
    let last: u64 = args[3].parse()?;
    if first == 0 || last < first || last - first >= 1000 {
        return Err("expected 1 <= first <= last with at most 1000 blocks".into());
    }
    let chain_spec = Arc::new(BaseChainSpec::mainnet());
    let runtime = RuntimeBuilder::new(RuntimeConfig::default()).build()?;
    let factory = ProviderFactoryBuilder::<BaseNode>::default()
        .open_read_only(chain_spec.clone(), &args[1], runtime)?;
    let evm: BaseEvmConfig = BaseEvmConfig::new(chain_spec.clone(), BaseRethReceiptBuilder::default());
    let consensus = BaseBeaconConsensus::new(chain_spec);
    for number in first..=last {
        let provider = factory.provider()?;
        let state = provider.history_by_block_number(number - 1)?;
        let block = factory.recovered_block(number.into(), TransactionVariant::NoHash)?
            .ok_or("block missing")?;
        let mut executor = evm.batch_executor(StateProviderDatabase(state));
        let result = executor.execute_one(&block)?;
        <BaseBeaconConsensus as FullConsensus<BasePrimitives>>::validate_block_post_execution(
            &consensus, &block, &result, None, None,
        )?;
        let bundle = executor.into_state().take_bundle();
        if bundle.reverts.len() != 1 { return Err("expected one block of reverts".into()); }
        let wiped: BTreeSet<Address> = bundle.reverts[0].iter()
            .filter(|(_, revert)| revert.wipe_storage)
            .map(|(address, _)| *address)
            .collect();
        let mut accounts = BTreeMap::new();
        let mut storage = BTreeMap::new();
        let mut codes = BTreeMap::new();
        for (address, account) in &bundle.state {
            let destroyed = wiped.contains(address) || account.status.was_destroyed();
            let mut changed_slots = BTreeMap::new();
            for (slot, value) in &account.storage {
                if account.info.is_some() && (value.is_changed() || destroyed && !value.present_value.is_zero()) {
                    changed_slots.insert(
                        format!("{:#x}", B256::from(slot.to_be_bytes::<32>())),
                        format!("{:#x}", B256::from(value.present_value.to_be_bytes::<32>())),
                    );
                }
            }
            if !changed_slots.is_empty() { storage.insert(format!("{address:#x}"), changed_slots); }
            if account.is_info_changed() || destroyed {
                let info = match &account.info {
                    Some(info) => {
                        if account.is_contract_changed() && info.code_hash != KECCAK_EMPTY {
                            if let Some(code) = bundle.contracts.get(&info.code_hash).or(info.code.as_ref()) {
                                codes.insert(format!("{:#x}", info.code_hash),
                                    format!("0x{}", hex::encode(code.original_bytes())));
                            }
                        }
                        serde_json::json!({
                            "exists": true,
                            "nonce": format!("0x{:x}", info.nonce),
                            "balance": format!("{:#x}", info.balance),
                            "code_hash": format!("{:#x}", info.code_hash),
                        })
                    }
                    None => serde_json::json!({"exists": false}),
                };
                accounts.insert(format!("{address:#x}"), serde_json::json!({
                    "state": info,
                    "previous_exists": account.original_info.is_some(),
                    "wiped_storage": destroyed,
                }));
            }
        }
        println!("{}", serde_json::json!({
            "block": number,
            "hash": format!("{:#x}", block.hash()),
            "parent_hash": format!("{:#x}", block.header().parent_hash()),
            "state_root": format!("{:#x}", block.header().state_root()),
            "timestamp": format!("0x{:x}", block.header().timestamp()),
            "accounts": accounts,
            "storage": storage,
            "codes": codes,
        }));
    }
    Ok(())
}
