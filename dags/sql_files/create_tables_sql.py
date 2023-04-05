# CREATE TABLES

PATIENT = """
	BEGIN;
	DROP TABLE IF EXISTS patient;
	CREATE TABLE IF NOT EXISTS patient (
		patient_id smallint,
		first_name varchar(100),
		last_name varchar(100),
		age smallint,
		sex smallint, 
		height smallint, 
		weight numeric(4,1),
		address varchar(500)
	) diststyle all;
	COMMIT;
"""

BLOOD_TEST = """
	BEGIN;
	DROP TABLE IF EXISTS bloodtest;
	CREATE TABLE IF NOT EXISTS bloodtest (
		bloodtest_id bigint identity(0, 1),
		ap_hi smallint,
		ap_lo smallint,
		cholesterol smallint,
		gluc smallint,
		smoke smallint,
		alco smallint,
		active smallint,
		cardio smallint,
		lab_id smallint,
		patient_id smallint,
		doc_id smallint,
		date bigint NOT NULL,
		CONSTRAINT bloodtest_pkey PRIMARY KEY (bloodtest_id)
	);
	COMMIT;
"""

DOCTOR = """
	BEGIN;
	DROP TABLE IF EXISTS doctor;
	CREATE TABLE IF NOT EXISTS doctor (
		doc_id smallint NOT NULL,
		doc_name varchar(300),
		clinic varchar(300),
		CONSTRAINT doc_pkey PRIMARY KEY (doc_id)
	) diststyle all;
	COMMIT;
"""

STAGING_LOGS = """
	BEGIN;
	DROP TABLE IF EXISTS staging_logs;
	CREATE TABLE IF NOT EXISTS  staging_logs (
		ap_hi smallint,
		ap_lo smallint,
		cholesterol smallint,
		gluc smallint,
		smoke smallint,
		alco smallint,
		active smallint,
		cardio smallint,
		lab_id smallint,
		city varchar(300),
		patient_id smallint        distkey,
		lab_name varchar(300)      sortkey,
		date bigint
	);
	COMMIT;
"""

STAGING_PATIENT = """
	BEGIN;
	DROP TABLE IF EXISTS staging_patient;
	CREATE TABLE IF NOT EXISTS staging_patient (
		id smallint                distkey,
		age smallint,
		sex smallint, 
		height smallint, 
		weight numeric(4,1),
		first_name varchar(200),
		last_name varchar(200),
		address varchar(500), 
		clinic varchar(300),
		doc_name varchar(300)       sortkey, 
		doc_id smallint
	);
	COMMIT;
"""

LAB = """
	BEGIN;
	DROP TABLE IF EXISTS laboratory;
	CREATE TABLE IF NOT EXISTS laboratory (
		lab_id smallint NOT NULL,
		lab_name varchar(300),
		city varchar(300),
		CONSTRAINT lab_pkey PRIMARY KEY (lab_id)
	) diststyle all;
	COMMIT;
"""
