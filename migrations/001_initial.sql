-- Initial RECAP schema; immutable after first application.

CREATE TABLE rejected_events (
	rejection_id VARCHAR(64) NOT NULL,
	record JSON NOT NULL,
	created_at FLOAT NOT NULL,
	published_at FLOAT,
	PRIMARY KEY (rejection_id)
);

CREATE TABLE runs (
	run_id VARCHAR(36) NOT NULL,
	manifest JSON NOT NULL,
	started_at FLOAT NOT NULL,
	finished_at FLOAT,
	status VARCHAR(20) DEFAULT 'registered' NOT NULL,
	execution JSON,
	PRIMARY KEY (run_id)
);

CREATE TABLE provider_payments (
	run_id VARCHAR(36) NOT NULL,
	idempotency_key VARCHAR(64) NOT NULL,
	transaction_id VARCHAR(36) NOT NULL,
	payload_hash VARCHAR(64) NOT NULL,
	outcome VARCHAR(16) NOT NULL,
	scenario VARCHAR(40) NOT NULL,
	submit_calls INTEGER NOT NULL,
	check_calls INTEGER NOT NULL,
	effect_count INTEGER NOT NULL,
	first_call_at FLOAT NOT NULL,
	visible_after FLOAT NOT NULL,
	PRIMARY KEY (run_id, idempotency_key),
	CONSTRAINT ck_provider_outcome CHECK (outcome IN ('SUCCESS','FAILED','UNKNOWN','NOT_FOUND')),
	CONSTRAINT ck_provider_counts CHECK (effect_count IN (0,1) AND submit_calls >= 0 AND check_calls >= 0),
	FOREIGN KEY(run_id) REFERENCES runs (run_id)
);

CREATE TABLE reconciliation_results (
	run_id VARCHAR(36) NOT NULL,
	transaction_id VARCHAR(36) NOT NULL,
	internal_outcome VARCHAR(16),
	reference_outcome VARCHAR(16),
	classification VARCHAR(40) NOT NULL,
	details JSON NOT NULL,
	checked_at FLOAT NOT NULL,
	PRIMARY KEY (run_id, transaction_id),
	FOREIGN KEY(run_id) REFERENCES runs (run_id)
);

CREATE TABLE source_references (
	run_id VARCHAR(36) NOT NULL,
	transaction_id VARCHAR(36) NOT NULL,
	idempotency_key VARCHAR(64) NOT NULL,
	event JSON NOT NULL,
	submitted_at FLOAT,
	PRIMARY KEY (run_id, transaction_id),
	CONSTRAINT uq_reference_key UNIQUE (run_id, idempotency_key),
	FOREIGN KEY(run_id) REFERENCES runs (run_id)
);

CREATE TABLE transactions (
	run_id VARCHAR(36) NOT NULL,
	transaction_id VARCHAR(36) NOT NULL,
	idempotency_key VARCHAR(64) NOT NULL,
	payload_hash VARCHAR(64) NOT NULL,
	originator VARCHAR(128) NOT NULL,
	destination VARCHAR(128) NOT NULL,
	amount_minor BIGINT NOT NULL,
	status VARCHAR(16) NOT NULL,
	created_at FLOAT NOT NULL,
	updated_at FLOAT NOT NULL,
	first_unresolved_at FLOAT,
	resolved_at FLOAT,
	attempts INTEGER DEFAULT '0' NOT NULL,
	duplicate_submissions INTEGER DEFAULT '0' NOT NULL,
	last_error TEXT,
	PRIMARY KEY (run_id, transaction_id),
	CONSTRAINT uq_transaction_key UNIQUE (run_id, idempotency_key),
	CONSTRAINT ck_transaction_state CHECK (status IN ('INITIATED','PROCESSING','SUCCESS','UNRESOLVED','FAILED')),
	CONSTRAINT ck_transaction_numbers CHECK (amount_minor >= 0 AND attempts >= 0 AND duplicate_submissions >= 0),
	FOREIGN KEY(run_id) REFERENCES runs (run_id)
);

CREATE TABLE inbox (
	run_id VARCHAR(36) NOT NULL,
	event_id VARCHAR(36) NOT NULL,
	transaction_id VARCHAR(36) NOT NULL,
	event_hash VARCHAR(64) NOT NULL,
	accepted_at FLOAT NOT NULL,
	metadata JSON NOT NULL,
	PRIMARY KEY (run_id, event_id),
	FOREIGN KEY(run_id, transaction_id) REFERENCES transactions (run_id, transaction_id)
);

CREATE TABLE recovery_jobs (
	run_id VARCHAR(36) NOT NULL,
	transaction_id VARCHAR(36) NOT NULL,
	status VARCHAR(16) NOT NULL,
	next_attempt_at FLOAT NOT NULL,
	lease_token VARCHAR(36),
	lease_until FLOAT,
	PRIMARY KEY (run_id, transaction_id),
	FOREIGN KEY(run_id, transaction_id) REFERENCES transactions (run_id, transaction_id),
	CONSTRAINT ck_job_state CHECK (status IN ('READY','LEASED','DONE','EXHAUSTED')),
	CONSTRAINT ck_job_lease CHECK (status != 'LEASED' OR (lease_token IS NOT NULL AND lease_until IS NOT NULL))
);

CREATE TABLE transaction_events (
	event_id VARCHAR(36) NOT NULL,
	run_id VARCHAR(36) NOT NULL,
	transaction_id VARCHAR(36) NOT NULL,
	event_type VARCHAR(40) NOT NULL,
	previous_state VARCHAR(16),
	next_state VARCHAR(16) NOT NULL,
	attempt_number INTEGER NOT NULL,
	timestamp FLOAT NOT NULL,
	details JSON NOT NULL,
	PRIMARY KEY (event_id),
	FOREIGN KEY(run_id, transaction_id) REFERENCES transactions (run_id, transaction_id),
	CONSTRAINT ck_event_next_state CHECK (next_state IN ('INITIATED','PROCESSING','SUCCESS','UNRESOLVED','FAILED')),
	CONSTRAINT ck_event_attempt CHECK (attempt_number >= 0)
);

CREATE INDEX ix_rejections_pending ON rejected_events (published_at);

CREATE INDEX ix_transaction_run_status ON transactions (run_id, status, updated_at);

CREATE INDEX ix_jobs_due ON recovery_jobs (status, next_attempt_at);

CREATE INDEX ix_jobs_expired ON recovery_jobs (status, lease_until);

CREATE INDEX ix_history_transaction_time ON transaction_events (run_id, transaction_id, timestamp);
