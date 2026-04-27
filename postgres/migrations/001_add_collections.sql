create extension if not exists pgcrypto;

create table if not exists collections (
    id uuid primary key default gen_random_uuid(),
    slug text unique not null,
    title text not null,
    summary text,
    readme_bucket text,
    readme_key text,
    published_at timestamptz not null default now(),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create index if not exists collections_listing_idx
    on collections (published_at desc, title asc);

alter table datasets
    add column if not exists collection_id uuid references collections(id) on delete set null;

alter table datasets
    add column if not exists sort_order integer not null default 0;

alter table datasets
    alter column published_at set default now();

update datasets
set published_at = coalesce(published_at, created_at, now())
where published_at is null;

alter table datasets
    alter column published_at set not null;

create index if not exists datasets_collection_idx
    on datasets (collection_id, sort_order, published_at desc, title asc);

create or replace function set_updated_at()
returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language plpgsql;

drop trigger if exists collections_set_updated_at on collections;
create trigger collections_set_updated_at
before update on collections
for each row execute function set_updated_at();
