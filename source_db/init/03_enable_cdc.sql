-- =============================================================================
-- CDC prerequisites for AWS DMS (PostgreSQL logical replication).
--
-- Server settings (NOT settable from SQL):
--   * Local Docker: passed as -c flags in source_db/docker-compose.postgres.yml
--       wal_level=logical, max_replication_slots=10, max_wal_senders=10
--   * Amazon RDS: DB parameter group (infra/aws/rds.tf)
--       rds.logical_replication=1 (reboot), wal_sender_timeout=0
-- =============================================================================

-- Least-privilege DMS user. The password is injected at runtime (psql variable locally,
-- AWS Secrets Manager in the cloud) — never committed.
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dms_cdc_user') THEN
        CREATE ROLE dms_cdc_user WITH LOGIN;
    END IF;
END $$;

-- Replication privilege: RDS uses the rds_replication role; self-managed PG uses REPLICATION.
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'rds_replication') THEN
        EXECUTE 'GRANT rds_replication TO dms_cdc_user';
    ELSE
        EXECUTE 'ALTER ROLE dms_cdc_user WITH REPLICATION';
    END IF;
END $$;

GRANT USAGE ON SCHEMA public TO dms_cdc_user;
GRANT SELECT ON public.customers, public.orders TO dms_cdc_user;

-- Full row images for UPDATE/DELETE in the WAL: deletes then carry every column
-- (not only the PK), and unchanged TOASTed values are not dropped from updates.
ALTER TABLE public.customers REPLICA IDENTITY FULL;
ALTER TABLE public.orders    REPLICA IDENTITY FULL;

-- Publication (used by the pglogical/pgoutput plugins; harmless for test_decoding).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'dms_cdc_publication') THEN
        CREATE PUBLICATION dms_cdc_publication FOR TABLE public.customers, public.orders;
    END IF;
END $$;

-- Pre-created logical replication slot referenced by the DMS source endpoint
-- (ExtraConnectionAttributes slotName=dms_cdc_slot). WARNING: an unconsumed slot retains
-- WAL indefinitely — alarm on slot lag (OPERATIONS.md#replication-slot-lag).
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'dms_cdc_slot') THEN
        PERFORM pg_create_logical_replication_slot('dms_cdc_slot', 'test_decoding');
    END IF;
END $$;

-- Sanity checks
SELECT name, setting FROM pg_settings WHERE name IN ('wal_level', 'max_replication_slots', 'max_wal_senders');
SELECT slot_name, plugin, active,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained_wal
FROM pg_replication_slots;
