-- Least-privilege role for position-exposure-service in the strict-security profile.
--
-- position-exposure-service shares the same PostgreSQL instance as risk-alert-service (ADR-028):
-- one server, one schema per service. POSTGRES_DB only creates a single database at initdb time,
-- and that database is risk_alert's, so unlike init-risk-alert-role.sql this script also has to
-- create the position_exposure database itself, not just its role and schema.
--
-- The dev profile runs the service as the database superuser, which is fine for a local stack and
-- wrong for anything modelling a cloud deployment. Here the service owns its schema and nothing
-- else: no CREATEDB, no CREATEROLE, no access to any other schema (ADR-028, ADR-030). The database
-- itself stays owned by the bootstrap role rather than by position_exposure_service: owning the
-- database would make position_exposure_service the owner of its own public schema too (PostgreSQL
-- 15+ owns public via pg_database_owner), which would grant CREATE there and contradict "no access
-- to any other schema".
--
-- POSTGRES_USER in the strict overlay is "postgres", not "position_exposure_service" (see
-- docker-compose.strict-security.yml): PostgreSQL bootstraps whatever POSTGRES_USER names as the
-- initdb superuser, and refuses to let that specific bootstrap role ever strip its own SUPERUSER
-- attribute ("permission denied to alter role: The bootstrap user must have the SUPERUSER
-- attribute", confirmed against postgres:16-alpine). So position_exposure_service is created here
-- instead, as a genuinely separate, non-superuser role, rather than hardened in place after the fact.
--
-- The role's password is read from the container's own POSITION_EXPOSURE_SERVICE_PASSWORD
-- environment variable via \getenv, distinct from both POSTGRES_PASSWORD (the bootstrap role's own
-- secret) and RISK_ALERT_SERVICE_PASSWORD (the sibling service's): sharing one secret across roles
-- would be a latent trap, since a single later pg_hba.conf edit granting one of them a wider network
-- route would then also grant that route under a credential already handled as another service's.
-- No credential is ever written to this tracked file either way.
--
-- Flyway still needs DDL on this schema, because the migration runs as the service at startup.

\getenv position_exposure_password POSITION_EXPOSURE_SERVICE_PASSWORD

CREATE ROLE position_exposure_service LOGIN PASSWORD :'position_exposure_password'
    NOSUPERUSER NOCREATEDB NOCREATEROLE;

CREATE DATABASE position_exposure;

\connect position_exposure

REVOKE ALL ON SCHEMA public FROM PUBLIC;

CREATE SCHEMA IF NOT EXISTS position_exposure AUTHORIZATION position_exposure_service;
ALTER ROLE position_exposure_service SET search_path TO position_exposure;

GRANT USAGE, CREATE ON SCHEMA position_exposure TO position_exposure_service;
