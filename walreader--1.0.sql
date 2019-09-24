/* contrib/walreader/walreader--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION walreader" to load this file. \quit

CREATE TYPE waldata AS
(
	timeline  INT4,
	walseg    TEXT,
	seg_off   INT4,
	page      INT4,
	page_off  INT4,
	rmgr      TEXT,
	rec_len   INT4,
	tot_len   INT4,
	tx        XID,
	lsn       TEXT,
	prev_lsn  TEXT,
	identify  TEXT,
	rmgr_desc TEXT
);

-- Read xlog records with wal segment file.
CREATE FUNCTION read_wal_segment(start_wal_segment text)
RETURNS SETOF waldata
AS 'MODULE_PATHNAME', 'read_wal_segment'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION read_wal_segment(start_wal_segment text, end_wal_segment text)
RETURNS SETOF waldata
AS 'MODULE_PATHNAME', 'read_wal_segment'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION read_wal_segment(start_wal_segment text,
                                 end_wal_segment text,
                                 walseg_directory text)
RETURNS SETOF waldata
AS 'MODULE_PATHNAME', 'read_wal_segment'
LANGUAGE C IMMUTABLE STRICT;

-- Read xlog records with lsn.
CREATE FUNCTION read_wal_lsn(start_lsn text)
RETURNS SETOF waldata
AS 'MODULE_PATHNAME', 'read_wal_lsn'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION read_wal_lsn(start_lsn text, end_lsn text)
RETURNS SETOF waldata
AS 'MODULE_PATHNAME', 'read_wal_lsn'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION read_wal_lsn(start_lsn text, end_lsn text, walseg_directory text)
RETURNS SETOF waldata
AS 'MODULE_PATHNAME', 'read_wal_lsn'
LANGUAGE C IMMUTABLE STRICT;
