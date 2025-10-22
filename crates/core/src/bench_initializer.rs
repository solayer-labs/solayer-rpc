use std::sync::{Arc, RwLock};

use borsh::{BorshDeserialize, BorshSerialize};
use crossbeam_channel::Sender;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use solana_pubkey::Pubkey;
use solana_sdk::{
    instruction::{AccountMeta, Instruction},
    message::Message,
    signature::Keypair,
    signer::{SeedDerivable, Signer},
    system_program,
    transaction::{SanitizedTransaction, Transaction},
};

use crate::bank::Bank;

pub const AMM_CONFIG_SEED: &str = "amm_config";
pub const POOL_SEED: &str = "pool";
pub const POOL_VAULT_SEED: &str = "pool_vault";
pub const AUTH_SEED: &str = "vault_and_lp_mint_auth_seed";
pub const POOL_LP_MINT_SEED: &str = "pool_lp_mint";
pub const OBSERVATION_SEED: &str = "observation";
pub const TOKEN_PROGRAM_ID: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
pub const ASSOCIATED_TOKEN_PROGRAM_ID: &str = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL";
pub const SWAP_PROGRAM_ID: &str = "rp7km3qAmYb8ciKKS23v5nmyYU9dFTc5RTAyx7zQSAz";
pub const CREATOR_POOL_FEE: &str = "9dyM5UXztbSui5RKw1CpSka9F1SUQxu4qzqN72gx3XAf";

#[derive(Deserialize, Serialize, Debug, BorshSerialize, BorshDeserialize)]
pub struct SwapBaseInputArgs {
    pub amount_in: u64,
    pub minimum_amount_out: u64,
}

#[derive(Deserialize, Serialize, Debug, BorshSerialize, BorshDeserialize)]
pub struct CreateAmmConfigArgs {
    pub index: u16,
    pub trade_fee_rate: u64,
    pub protocol_fee_rate: u64,
    pub fund_fee_rate: u64,
    pub create_fee: u64,
}
#[derive(Deserialize, Serialize, Debug, BorshSerialize, BorshDeserialize)]
pub struct InitializeArgs {
    pub init_amount_0: u64,
    pub init_amount_1: u64,
    pub open_time: u64,
}

#[cfg(feature = "devnet")]
pub struct BenchInitializer;

#[cfg(feature = "devnet")]
impl BenchInitializer {
    pub fn init_bench(bank: Arc<RwLock<Bank>>, tx_sender: Sender<(SanitizedTransaction, u64)>) {
        let faucet = Pubkey::from_str_const("FUND4EFuH8XaPmFkFLvABVQzfBZ2GQ7grYHhWV6ZYTQm");
        bank.write().unwrap().init_accounts_for_test(&faucet, false);

        // init 400k accounts
        let keypairs = (0..400_000)
            .into_par_iter()
            .map(|i: u32| {
                let mut buffer = [0x42u8; 32];
                buffer[0..4].copy_from_slice(&i.to_le_bytes());
                Keypair::from_seed(&buffer).unwrap()
            })
            .collect::<Vec<_>>();

        let bank_rw = bank.read().unwrap();
        bank_rw.init_bench_shared_accounts();
        for keypair in keypairs.iter() {
            bank_rw.init_accounts_for_test(&keypair.pubkey(), true);
        }

        // init swap related accounts
        // this needs to be a fixed keypair for testing purposes
        let swap_owner_keypair = Keypair::from_seed(&[
            25, 34, 41, 174, 170, 10, 138, 121, 104, 246, 102, 252, 148, 193, 220, 194, 176, 254, 158, 164, 71, 105,
            77, 166, 58, 37, 73, 96, 200, 144, 184, 224, 61, 209, 5, 4, 17, 111, 188, 154, 93, 159, 26, 67, 34, 157,
            177, 232, 104, 239, 102, 247, 16, 215, 36, 241, 118, 115, 12, 207, 251, 33, 90, 127,
        ])
        .unwrap();

        bank_rw.init_accounts_for_test(&swap_owner_keypair.pubkey(), true);

        // pool fee account, wsol account of this account needs to be initialized
        let pool_fee_owner_keypair = Keypair::from_seed(&[
            115, 178, 152, 28, 56, 230, 190, 55, 254, 217, 213, 210, 105, 166, 93, 129, 31, 62, 170, 139, 206, 231,
            223, 220, 114, 45, 81, 115, 151, 117, 156, 13, 132, 129, 202, 2, 162, 84, 207, 50, 253, 200, 139, 2, 10,
            179, 193, 86, 233, 4, 239, 58, 207, 247, 219, 237, 90, 206, 8, 41, 3, 224, 223, 125,
        ])
        .unwrap();

        println!("pool_fee_owner_keypair: {}", pool_fee_owner_keypair.pubkey());
        bank_rw.init_accounts_for_test(&pool_fee_owner_keypair.pubkey(), true);

        drop(bank_rw);

        // init pool
        let init_tx = {
            let pubkey = swap_owner_keypair.pubkey();
            let create_amm_config_ix =
                Self::build_create_amm_config_tx(Pubkey::from_str_const(SWAP_PROGRAM_ID), pubkey, 0, 0, 0, 0, 0);

            let init_pool_ix = Self::build_init_pool_tx(
                Pubkey::from_str_const(SWAP_PROGRAM_ID),
                pubkey,
                Pubkey::from_str_const("LAYER4xPpTCb3QL8S9u41EAhAX7mhBn8Q6xMTwY2Yzc"),
                Pubkey::from_str_const(TOKEN_PROGRAM_ID),
                Pubkey::from_str_const("So11111111111111111111111111111111111111112"),
                Pubkey::from_str_const(TOKEN_PROGRAM_ID),
            );

            let latest_blockhash = bank.read().unwrap().current_blockhash();
            let tx = Transaction::new(
                &[swap_owner_keypair],
                Message::new(&[create_amm_config_ix, init_pool_ix], Some(&pubkey)),
                latest_blockhash,
            );
            SanitizedTransaction::try_from_legacy_transaction(tx, &std::collections::HashSet::new()).unwrap()
        };

        tx_sender.send((init_tx, 0)).unwrap();
    }

    pub fn get_discriminator(name: &str) -> [u8; 8] {
        let hash = Sha256::digest(format!("global:{}", name).as_bytes());
        let mut discriminator = [0u8; 8];
        discriminator.copy_from_slice(&hash[..8]);
        discriminator
    }

    fn build_create_amm_config_tx(
        program_id: Pubkey,
        owner: Pubkey,
        config_index: u16,
        trade_fee_rate: u64,
        protocol_fee_rate: u64,
        fund_fee_rate: u64,
        create_fee: u64,
    ) -> Instruction {
        let (address, _) =
            Pubkey::find_program_address(&[AMM_CONFIG_SEED.as_bytes(), &config_index.to_be_bytes()], &program_id);

        // Compute instruction data
        let mut data = Self::get_discriminator("create_amm_config").to_vec();
        let args = CreateAmmConfigArgs {
            index: config_index,
            trade_fee_rate,
            protocol_fee_rate,
            fund_fee_rate,
            create_fee,
        };
        data.extend(args.try_to_vec().unwrap());

        // Accounts (must match Anchor's struct order!)
        let accounts = vec![
            AccountMeta::new(owner, true),                        // my_account
            AccountMeta::new(address, false),                     // user
            AccountMeta::new_readonly(system_program::ID, false), // system_program
        ];

        // Create instruction
        Instruction {
            program_id,
            accounts,
            data,
        }
    }

    fn build_init_pool_tx(
        program_id: Pubkey,
        creator: Pubkey,
        token_0_mint: Pubkey,
        token_0_program: Pubkey,
        token_1_mint: Pubkey,
        token_1_program: Pubkey,
    ) -> Instruction {
        let init_amt0: u64 = 5_000_000_000_000;
        let init_amt1: u64 = 5_000_000_000_000;

        let amm_config_index = 0u16;
        let (amm_config_key, __bump) = Pubkey::find_program_address(
            &[AMM_CONFIG_SEED.as_bytes(), &amm_config_index.to_be_bytes()],
            &program_id,
        );

        let (pool_account_key, _) = Pubkey::find_program_address(
            &[
                POOL_SEED.as_bytes(),
                amm_config_key.to_bytes().as_ref(),
                token_0_mint.to_bytes().as_ref(),
                token_1_mint.to_bytes().as_ref(),
            ],
            &program_id,
        );

        let (authority, _) = Pubkey::find_program_address(&[AUTH_SEED.as_bytes()], &program_id);
        let (token_0_vault, __bump) = Pubkey::find_program_address(
            &[
                POOL_VAULT_SEED.as_bytes(),
                pool_account_key.to_bytes().as_ref(),
                token_0_mint.to_bytes().as_ref(),
            ],
            &program_id,
        );
        let (token_1_vault, __bump) = Pubkey::find_program_address(
            &[
                POOL_VAULT_SEED.as_bytes(),
                pool_account_key.to_bytes().as_ref(),
                token_1_mint.to_bytes().as_ref(),
            ],
            &program_id,
        );

        let (lp_mint_key, __bump) = Pubkey::find_program_address(
            &[POOL_LP_MINT_SEED.as_bytes(), pool_account_key.to_bytes().as_ref()],
            &program_id,
        );
        let (observation_key, __bump) = Pubkey::find_program_address(
            &[OBSERVATION_SEED.as_bytes(), pool_account_key.to_bytes().as_ref()],
            &program_id,
        );

        let (creator_token_0, _) = Pubkey::find_program_address(
            &[
                creator.to_bytes().as_ref(),
                Pubkey::from_str_const(TOKEN_PROGRAM_ID).to_bytes().as_ref(),
                token_0_mint.to_bytes().as_ref(),
            ],
            &Pubkey::from_str_const(ASSOCIATED_TOKEN_PROGRAM_ID),
        );

        let (creator_token_1, _) = Pubkey::find_program_address(
            &[
                creator.to_bytes().as_ref(),
                Pubkey::from_str_const(TOKEN_PROGRAM_ID).to_bytes().as_ref(),
                token_1_mint.to_bytes().as_ref(),
            ],
            &Pubkey::from_str_const(ASSOCIATED_TOKEN_PROGRAM_ID),
        );

        println!("creator_token_0: {}", creator_token_0);
        println!("creator_token_1: {}", creator_token_1);

        let (creator_lp_token, _) = Pubkey::find_program_address(
            &[
                creator.to_bytes().as_ref(),
                Pubkey::from_str_const(TOKEN_PROGRAM_ID).to_bytes().as_ref(),
                lp_mint_key.to_bytes().as_ref(),
            ],
            &Pubkey::from_str_const(ASSOCIATED_TOKEN_PROGRAM_ID),
        );

        // Compute instruction data
        let mut data = Self::get_discriminator("initialize").to_vec();
        let args = InitializeArgs {
            init_amount_0: init_amt0,
            init_amount_1: init_amt1,
            open_time: 0,
        };
        data.extend(args.try_to_vec().unwrap());

        let accounts = vec![
            AccountMeta::new(creator, true),
            AccountMeta::new(amm_config_key, false),
            AccountMeta::new(authority, false),
            AccountMeta::new(pool_account_key, false),
            AccountMeta::new(token_0_mint, false),
            AccountMeta::new(token_1_mint, false),
            AccountMeta::new(lp_mint_key, false),
            AccountMeta::new(creator_token_0, false),
            AccountMeta::new(creator_token_1, false),
            AccountMeta::new(creator_lp_token, false),
            AccountMeta::new(token_0_vault, false),
            AccountMeta::new(token_1_vault, false),
            AccountMeta::new(Pubkey::from_str_const(CREATOR_POOL_FEE), false),
            AccountMeta::new(observation_key, false),
            AccountMeta::new_readonly(Pubkey::from_str_const(TOKEN_PROGRAM_ID), false),
            AccountMeta::new_readonly(token_0_program, false),
            AccountMeta::new_readonly(token_1_program, false),
            AccountMeta::new_readonly(Pubkey::from_str_const(ASSOCIATED_TOKEN_PROGRAM_ID), false),
            AccountMeta::new_readonly(system_program::ID, false),
            AccountMeta::new_readonly(
                Pubkey::from_str_const("SysvarRent111111111111111111111111111111111"),
                false,
            ),
        ];

        // Create instruction
        Instruction {
            program_id,
            accounts,
            data,
        }
    }
}
