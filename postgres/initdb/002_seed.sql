-- Demo-only seed data for local testing and first-run validation.
-- Do not reuse these UUIDs, token hashes, object keys, or example labels in production.

insert into collections (
    id,
    slug,
    title,
    summary,
    published_at
) values
    (
        '99999999-9999-9999-9999-999999999991',
        'gaza-nowpop-apr-2026',
        'Gaza NowPop, April 2026',
        'April 2026 release package for Gaza population estimates, partner files, and supporting notes.',
        '2026-04-01T00:00:00Z'
    )
on conflict (id) do nothing;

insert into datasets (
    id,
    collection_id,
    slug,
    title,
    summary,
    dataset_role,
    classification,
    visibility,
    storage_bucket,
    storage_key,
    mime_type,
    sort_order,
    published_at
) values
    (
        '11111111-1111-1111-1111-111111111111',
        '99999999-9999-9999-9999-999999999991',
        'gaza-population-estimates-apr-2026',
        'Gaza Population Estimates, April 2026',
        'Public governorate-level population estimates for operational planning.',
        'data',
        'public',
        'listed',
        'gazanowpop',
        'gaza/population-estimates-apr-2026.csv',
        'text/csv',
        10,
        '2026-04-01T00:00:00Z'
    ),
    (
        '22222222-2222-2222-2222-222222222222',
        '99999999-9999-9999-9999-999999999991',
        'gaza-partner-briefing-alpha-apr-2026',
        'Gaza Partner Briefing Alpha, April 2026',
        'Restricted partner release with additional breakdowns.',
        'data',
        'restricted',
        'listed',
        'gazanowpop',
        'restricted/partner-alpha/briefing-apr-2026.xlsx',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        20,
        '2026-04-02T00:00:00Z'
    ),
    (
        '33333333-3333-3333-3333-333333333333',
        '99999999-9999-9999-9999-999999999991',
        'gaza-method-notes-confidential-apr-2026',
        'Gaza Method Notes, Confidential, April 2026',
        'Confidential reference notes listed for transparency but never downloadable.',
        'documentation',
        'confidential',
        'listed',
        'gazanowpop',
        'strictly-confidential/method-notes-apr-2026.pdf',
        'application/pdf',
        30,
        '2026-04-03T00:00:00Z'
    ),
    (
        '44444444-4444-4444-4444-444444444444',
        '99999999-9999-9999-9999-999999999991',
        'gaza-public-method-notes-apr-2026',
        'Gaza Method Notes, Public, April 2026',
        'Public-facing documentation for the April 2026 Gaza release.',
        'documentation',
        'public',
        'listed',
        'gazanowpop',
        'docs/public-method-notes-apr-2026.html',
        'text/html',
        5,
        '2026-04-01T00:00:00Z'
    )
on conflict (id) do nothing;

insert into dataset_tags (dataset_id, tag) values
    ('11111111-1111-1111-1111-111111111111', 'public'),
    ('11111111-1111-1111-1111-111111111111', 'population'),
    ('11111111-1111-1111-1111-111111111111', 'gaza'),
    ('22222222-2222-2222-2222-222222222222', 'restricted'),
    ('22222222-2222-2222-2222-222222222222', 'partner-alpha'),
    ('44444444-4444-4444-4444-444444444444', 'documentation'),
    ('44444444-4444-4444-4444-444444444444', 'methods'),
    ('33333333-3333-3333-3333-333333333333', 'confidential'),
    ('33333333-3333-3333-3333-333333333333', 'methods')
on conflict do nothing;

insert into access_tokens (
    id,
    token_hash,
    label,
    status,
    expires_at
) values
    (
        'aaaaaaa1-aaaa-aaaa-aaaa-aaaaaaaaaaa1',
        '53d3556e6ea2d7e42a245cda103408d459dc7aee354f782bef243f53ce18d601',
        'Partner Alpha',
        'active',
        '2026-12-31T23:59:59Z'
    ),
    (
        'bbbbbbb2-bbbb-bbbb-bbbb-bbbbbbbbbbb2',
        '761ff32146fc02ddb130c2ebf71ba43f588c984e0ee64b0c6e2f36169e4a7e23',
        'Partner Beta',
        'active',
        null
    )
on conflict (id) do nothing;

insert into token_grants (
    id,
    token_id,
    bucket,
    key_prefix,
    effect
) values
    (
        'ccccccc3-cccc-cccc-cccc-ccccccccccc3',
        'aaaaaaa1-aaaa-aaaa-aaaa-aaaaaaaaaaa1',
        'gazanowpop',
        'restricted/partner-alpha/',
        'allow'
    ),
    (
        'ddddddd4-dddd-dddd-dddd-ddddddddddd4',
        'bbbbbbb2-bbbb-bbbb-bbbb-bbbbbbbbbbb2',
        'gazanowpop',
        'restricted/shared/',
        'allow'
    )
on conflict (id) do nothing;
