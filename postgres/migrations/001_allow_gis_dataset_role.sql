do $$
declare
    constraint_name text;
begin
    select con.conname
    into constraint_name
    from pg_constraint con
    join pg_class rel on rel.oid = con.conrelid
    join pg_namespace nsp on nsp.oid = rel.relnamespace
    where nsp.nspname = 'public'
      and rel.relname = 'datasets'
      and con.contype = 'c'
      and pg_get_constraintdef(con.oid) like '%dataset_role%';

    if constraint_name is not null then
        execute format('alter table public.datasets drop constraint %I', constraint_name);
    end if;

    alter table public.datasets
        add constraint datasets_dataset_role_check
        check (dataset_role in ('data', 'documentation', 'visuals', 'GIS'));
end
$$;
