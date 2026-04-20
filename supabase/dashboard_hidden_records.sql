create table if not exists public.dashboard_hidden_records (
  record_key text primary key,
  payload jsonb not null,
  hidden_by text,
  hidden_at timestamptz not null default timezone('utc', now())
);

alter table public.dashboard_hidden_records enable row level security;

create policy "dashboard_hidden_records_select_all"
on public.dashboard_hidden_records
for select
using (true);

create policy "dashboard_hidden_records_insert_authenticated"
on public.dashboard_hidden_records
for insert
to authenticated
with check (true);

create policy "dashboard_hidden_records_update_authenticated"
on public.dashboard_hidden_records
for update
to authenticated
using (true)
with check (true);

create policy "dashboard_hidden_records_delete_authenticated"
on public.dashboard_hidden_records
for delete
to authenticated
using (true);
