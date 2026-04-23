create extension if not exists pgcrypto;

create table if not exists datasets (
    id uuid primary key default gen_random_uuid(),
    slug text unique not null,
    title text not null,
    summary text,
    classification text not null check (classification in ('public', 'restricted', 'confidential')),
    visibility text not null default 'listed' check (visibility in ('listed', 'hidden')),
    storage_bucket text not null,
    storage_key text not null,
    direct_public_url text,
    mime_type text,
    file_size_bytes bigint,
    checksum_sha256 text,
    published_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create unique index if not exists datasets_storage_idx
    on datasets (storage_bucket, storage_key);

create index if not exists datasets_listing_idx
    on datasets (visibility, classification, published_at desc);

create table if not exists dataset_tags (
    dataset_id uuid not null references datasets(id) on delete cascade,
    tag text not null,
    primary key (dataset_id, tag)
);

create table if not exists access_tokens (
    id uuid primary key default gen_random_uuid(),
    token_hash text unique not null,
    label text not null,
    status text not null default 'active' check (status in ('active', 'revoked', 'expired')),
    expires_at timestamptz,
    created_at timestamptz not null default now()
);

create index if not exists access_tokens_lookup_idx
    on access_tokens (token_hash, status, expires_at);

create table if not exists token_grants (
    id uuid primary key default gen_random_uuid(),
    token_id uuid not null references access_tokens(id) on delete cascade,
    dataset_id uuid references datasets(id) on delete cascade,
    classification text check (classification in ('public', 'restricted', 'confidential')),
    bucket text,
    key_prefix text,
    effect text not null default 'allow' check (effect in ('allow')),
    created_at timestamptz not null default now(),
    check (
        dataset_id is not null
        or classification is not null
        or bucket is not null
    )
);

create index if not exists token_grants_token_idx
    on token_grants (token_id);

create index if not exists token_grants_dataset_idx
    on token_grants (dataset_id);

create or replace function set_updated_at()
returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language plpgsql;

drop trigger if exists datasets_set_updated_at on datasets;
create trigger datasets_set_updated_at
before update on datasets
for each row execute function set_updated_at();
