// @generated automatically by Diesel CLI.

diesel::table! {
    domains (name) {
        name -> Varchar,
        parent -> Varchar,
        expiration_timestamp_ms -> Int8,
        nft_id -> Varchar,
        field_id -> Varchar,
        target_address -> Nullable<Varchar>,
        data -> Json,
        last_checkpoint_updated -> Int8,
    }
}
