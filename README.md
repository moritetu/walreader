walreader
=========

walreader is a postgresql extension to read wal records with sql function. This extension will help you to study wal internal structure.

walreader was created with reference to pg_waldump. pg_waldump will be lighter, more feature rich and easier to use.

Installation
------------
 
 You can install walreader extension with the following commands.
 
 ```bash
 $ git clone https://github.com/moritetu/walreader.git
 $ cd walreader
 $ make USE_PGXS=1 install
 ```
 
 Next, log in a database and install the extension with `CREATE EXTENSION` command.
 
 ```
 postgres=# CREATE EXTENSION walreader;
 ```
 
Settings
--------

### walreader.default_wal_directory

Specifies the directory where we read wal segment files. Default is `XLOGDIR`, which means `pg_wal` in version 10 or later.

You can switch value of this parameter in session.

```
postgres=# set walreader.default_wal_directory to '/path/to/waldir';
```

### walreader.read_limit

Maximum number of reading wal. Default is `0` and it means no limit.

You can specify in the range of 0 to `INT_MAX`.

 
Usage
-----

### Read wal records from segment file

**SQL Function**

```
read_wal_segment(start_wal_segment [, end_wal_segment [, wal_directory]]);
```

**Example**

```sql
postgres=# select * from read_wal_segment('000000010000000000000004');
 timeline |          walseg          | seg_off | page | page_off |    rmgr     | rec_len | tot_len | tx  |    lsn     |  prev_lsn  |      identify       |                                                                                                   rmgr_desc                                                                                                   
----------+--------------------------+---------+------+----------+-------------+---------+---------+-----+------------+------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        1 | 000000010000000000000004 |      40 |    1 |       40 | Standby     |      50 |      50 |   0 | 0/04000028 | 0/03000110 | RUNNING_XACTS       | nextXid 488 latestCompletedXid 487 oldestRunningXid 488
        1 | 000000010000000000000004 |      96 |    1 |       96 | Standby     |      50 |      50 |   0 | 0/04000060 | 0/04000028 | RUNNING_XACTS       | nextXid 488 latestCompletedXid 487 oldestRunningXid 488
        1 | 000000010000000000000004 |     152 |    1 |      152 | XLOG        |     114 |     114 |   0 | 0/04000098 | 0/04000060 | CHECKPOINT_ONLINE   | redo 0/4000028; tli 1; prev tli 1; fpw true; xid 0:488; oid 24576; multi 1; offset 0; oldest xid 479 in DB 1; oldest multi 1 in DB 1; oldest/newest commit timestamp xid: 0/0; oldest running xid 488; online
        1 | 000000010000000000000004 |     272 |    1 |      272 | Standby     |      50 |      50 |   0 | 0/04000110 | 0/04000098 | RUNNING_XACTS       | nextXid 488 latestCompletedXid 487 oldestRunningXid 488
        1 | 000000010000000000000004 |     328 |    1 |      328 | Heap        |      54 |     150 | 488 | 0/04000148 | 0/04000110 | INSERT              | off 2 flags 0x00
        1 | 000000010000000000000004 |     480 |    1 |      480 | Transaction |      34 |      34 | 488 | 0/040001E0 | 0/04000148 | COMMIT              | 2019-09-15 11:56:40.963685+09
        1 | 000000010000000000000004 |     520 |    1 |      520 | Standby     |      50 |      50 |   0 | 0/04000208 | 0/040001E0 | RUNNING_XACTS       | nextXid 489 latestCompletedXid 488 oldestRunningXid 489
        1 | 000000010000000000000004 |     576 |    1 |      576 | XLOG        |     114 |     114 |   0 | 0/04000240 | 0/04000208 | CHECKPOINT_SHUTDOWN | redo 0/4000240; tli 1; prev tli 1; fpw true; xid 0:489; oid 24576; multi 1; offset 0; oldest xid 479 in DB 1; oldest multi 1 in DB 1; oldest/newest commit timestamp xid: 0/0; oldest running xid 0; shutdown
        1 | 000000010000000000000004 |     696 |    1 |      696 | Standby     |      50 |      50 |   0 | 0/040002B8 | 0/04000240 | RUNNING_XACTS       | nextXid 489 latestCompletedXid 488 oldestRunningXid 489
        1 | 000000010000000000000004 |     752 |    1 |      752 | XLOG        |      24 |      24 |   0 | 0/040002F0 | 0/040002B8 | SWITCH              | 
(10 rows)

postgres=# select * from read_wal_segment(pg_walfile_name(pg_current_wal_lsn())) limit 1;
WARNING:  invalid record length at 0/135D60E0: wanted 24, got 0
 timeline |          walseg          | seg_off | page | page_off |  rmgr   | rec_len | tot_len | tx |    lsn     |  prev_lsn  |   identify    |                        rmgr_desc                        
----------+--------------------------+---------+------+----------+---------+---------+---------+----+------------+------------+---------------+---------------------------------------------------------
        1 | 000000010000000000000013 |      40 |    1 |       40 | Standby |      50 |      50 |  0 | 0/13000028 | 0/12488EA0 | RUNNING_XACTS | nextXid 537 latestCompletedXid 536 oldestRunningXid 537
(1 row)
```
 
### Read wal records with lsn

**SQL Function**

```
read_wal_lsn(start_wal_lsn [, end_wal_lsn [, wal_directory]]);
```

**Example**

```
 postgres=# select * from read_wal_lsn('0/04000208');
 timeline |          walseg          | seg_off | page | page_off |  rmgr   | rec_len | tot_len | tx |    lsn     |  prev_lsn  |      identify       |                                                                                                   rmgr_desc                                                                                                   
----------+--------------------------+---------+------+----------+---------+---------+---------+----+------------+------------+---------------------+---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        1 | 000000010000000000000004 |     520 |    1 |      520 | Standby |      50 |      50 |  0 | 0/04000208 | 0/040001E0 | RUNNING_XACTS       | nextXid 489 latestCompletedXid 488 oldestRunningXid 489
        1 | 000000010000000000000004 |     576 |    1 |      576 | XLOG    |     114 |     114 |  0 | 0/04000240 | 0/04000208 | CHECKPOINT_SHUTDOWN | redo 0/4000240; tli 1; prev tli 1; fpw true; xid 0:489; oid 24576; multi 1; offset 0; oldest xid 479 in DB 1; oldest multi 1 in DB 1; oldest/newest commit timestamp xid: 0/0; oldest running xid 0; shutdown
        1 | 000000010000000000000004 |     696 |    1 |      696 | Standby |      50 |      50 |  0 | 0/040002B8 | 0/04000240 | RUNNING_XACTS       | nextXid 489 latestCompletedXid 488 oldestRunningXid 489
        1 | 000000010000000000000004 |     752 |    1 |      752 | XLOG    |      24 |      24 |  0 | 0/040002F0 | 0/040002B8 | SWITCH              | 
(4 rows)


```