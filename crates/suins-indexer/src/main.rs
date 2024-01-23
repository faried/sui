// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use anyhow::Result;
use async_trait::async_trait;
use diesel::{dsl::sql, upsert::excluded, ExpressionMethods, RunQueryDsl};
use prometheus::Registry;
use std::{path::PathBuf, str::FromStr};
use sui_data_ingestion::{
    DataIngestionMetrics, FileProgressStore, IndexerExecutor, Worker, WorkerPool,
};
use sui_json_rpc::name_service::{Domain, NameRecord};
use sui_types::{
    base_types::{ObjectID, SuiAddress},
    dynamic_field::Field,
    full_checkpoint_content::CheckpointData,
    object::Object,
};
use suins_indexer::{get_connection_pool, models::VerifiedDomain, PgConnectionPool};

use dotenvy::dotenv;
use std::env;
use tokio::sync::oneshot;

struct NameRecordChange {
    // the field we're processing for updates.
    field: Field<Domain, NameRecord>,
    // the field_id so we can track deletions
    field_id: ObjectID,
    // check if we deleted or not
    deleted: bool,
}

struct DummyWorker {
    pg_pool: PgConnectionPool,
    name_record_df_table_id: String,
    name_record_type: String,
}

const NAME_RECORD_DF_TABLE_ID: &str =
    "0xe64cd9db9f829c6cc405d9790bd71567ae07259855f4fba6f02c84f52298c106";
const NAME_RECORD_TYPE: &str = concat!(
    "0xd22b24490e0bae52676651b4f56660a5ff8022a2576e0089f79b3c88d44e08f0",
    "::name_record::NameRecord"
);

/// Allows us to format a SuiNS specific query for updating the DB entries
/// only if the checkpoint is newer than the last checkpoint we have in the DB.
/// And only if the expiration timestamp is either the same or in the future.
/// Doing that, we do not care about the order of execution and we can run tasks in multiple threads.
fn format_update_field_query(field: &str) -> String {
    let update_case = "CASE WHEN excluded.last_checkpoint_updated > domains.last_checkpoint_updated AND excluded.expiration_timestamp_ms >= domains.expiration_timestamp_ms";

    format!("{update_case} THEN excluded.{field} ELSE domains.{field} END")
}

impl DummyWorker {
    /// Converts an array of objects to an array of NameRecord objects to save in the DB.
    fn parse_name_record_changes(&self, objects: Vec<&Object>) -> Vec<NameRecordChange> {
        objects
            .iter()
            .filter(|o| {
                // Filter single owner.
                o.get_single_owner().is_some()
                // Single owner has to be the TABLE of the registry.
                    && o.get_single_owner().unwrap() == SuiAddress::from_str(&self.name_record_df_table_id).unwrap()
                    // Valid struct tag 
                    && o.struct_tag().is_some()
                    // Check that the struct tag is a name record.
                    && o.struct_tag()
                        .unwrap()
                        .to_canonical_string(true)
                        .contains(&self.name_record_type)
            })
            // map all the objects to NameRecord type.
            // TODO: handle deletions!
            .map(|x| NameRecordChange {
                field: x.to_rust::<Field<Domain, NameRecord>>().unwrap(),
                field_id: x.id().clone(),
                deleted: false
            })
            .collect()
    }

    /// Tries to write a name record in the database!
    fn write_name_record_to_db(
        &self,
        names_to_update: Vec<NameRecordChange>,
        checkpoint_seq_num: u64,
    ) -> Result<()> {
        use suins_indexer::schema::domains;

        let mut updates: Vec<VerifiedDomain> = vec![];

        for name_record_change in names_to_update.iter() {
            let name_record = &name_record_change.field;

            let parent = name_record.name.parent().to_string();
            let nft_id = name_record.value.nft_id.bytes.to_string();

            updates.push(VerifiedDomain {
                field_id: name_record_change.field_id.to_string(),
                name: name_record.name.to_string(),
                parent,
                expiration_timestamp_ms: name_record.value.expiration_timestamp_ms as i64,
                nft_id,
                target_address: if name_record.value.target_address.is_some() {
                    Some(SuiAddress::to_string(
                        &name_record.value.target_address.unwrap(),
                    ))
                } else {
                    None
                },
                data: serde_json::to_value(&name_record.value.data).unwrap(),
                last_checkpoint_updated: checkpoint_seq_num as i64,
            });
        }

        // Bulk insert all updates and override with data.
        // TODO: Consider adding a `match` case on conflict to make sure we only save latest data.
        diesel::insert_into(domains::table)
            .values(
                &updates
            )
            .on_conflict(domains::name)
            .do_update()
            .set((
                domains::expiration_timestamp_ms.eq(sql(&format_update_field_query("expiration_timestamp_ms"))),
                domains::nft_id.eq(sql(&format_update_field_query("nft_id"))),
                domains::target_address.eq(sql(&format_update_field_query("target_address"))),
                domains::data.eq(sql(&format_update_field_query("data"))),
                domains::last_checkpoint_updated.eq(sql(&format_update_field_query("last_checkpoint_updated"))),
                domains::field_id.eq(sql(&format_update_field_query("field_id"))),
            ))
            .execute(&mut self.pg_pool.get().unwrap())
            .expect(&format!("TODO: Implement error handling"));

        Ok(())
    }
}

#[async_trait]
impl Worker for DummyWorker {
    async fn process_checkpoint(&self, checkpoint: CheckpointData) -> Result<()> {
        let output_objects = checkpoint.output_objects();

        let name_records = self.parse_name_record_changes(output_objects);

        if name_records.is_empty() {
            return Ok(());
        }

        self.write_name_record_to_db(name_records, checkpoint.checkpoint_summary.sequence_number)?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();

    // aws setup from .env file
    let aws_key_id = env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID must be set");
    let aws_secret_access_key =
        env::var("AWS_ACCESS_SECRET_KEY").expect("AWS_ACCESS_SECRET_KEY must be set");
    let aws_session_token = env::var("AWS_SESSION_TOKEN").expect("AWS_SESSION_TOKEN must be set");

    // let backfill_progress_file_path = env::var("BACKFILL_PROGRESS_FILE_PATH").unwrap_or("/tmp/backfill_progress".to_string());
    // let checkpoints_dir = env::var("CHECKPOINTS_DIR").unwrap_or("/tmp/checkpoints".to_string());

    let (_exit_sender, exit_receiver) = oneshot::channel();
    let progress_store =
        FileProgressStore::new(PathBuf::from("/Users/manosliolios/tmp/backfill_progress"));
    let metrics = DataIngestionMetrics::new(&Registry::new());

    let mut executor = IndexerExecutor::new(progress_store, 1, metrics);

    let pg_pool = get_connection_pool();

    let worker_pool = WorkerPool::new(
        DummyWorker {
            pg_pool,
            name_record_df_table_id: NAME_RECORD_DF_TABLE_ID.to_owned(),
            name_record_type: NAME_RECORD_TYPE.to_owned(),
        },
        "suins_indexing".to_string(), /* task name used as a key in the progress store */
        500,                          /* concurrency */
    );
    executor.register(worker_pool).await?;

    executor
        .run(
            PathBuf::from("/Users/manosliolios/tmp/checkpoints"), /* directory should exist but can be empty */
            Some("https://s3.us-west-2.amazonaws.com/mysten-mainnet-checkpoints".to_string()),
            vec![
                (
                    "aws_access_key_id".to_string(),
                    aws_key_id
                ),
                (
                    "aws_secret_access_key".to_string(),
                    aws_secret_access_key
                ),
                (
                    "aws_session_token".to_string(),
                    aws_session_token
                   )
            ],
            exit_receiver,
        )
        .await?;
    Ok(())
}
