/*-------------------------------------------------------------------------
 *
 * walreader.c
 *
 * walreader was created with reference to pg_waldump mainly
 * for learning purposes.
 *
 *	  contrib/walreader/walreader.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>
#include <sys/stat.h>

#include "fmgr.h"
#include "funcapi.h"
#include "catalog/pg_type.h"
#include "executor/tuptable.h"
#include "access/xlog_internal.h"
#include "access/xlogdefs.h"
#include "access/xlogreader.h"
#include "utils/tuplestore.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "lib/stringinfo.h"
#include "storage/fd.h"


PG_MODULE_MAGIC;


/*
 * GUC variables
 */

/* Default wal directory */
static char *walreader_default_wal_directory = NULL;
/* Limit record num to read */
static int walreader_read_limit;


/*
 * Private data passed to XLogReaderState.
 */
typedef struct WalReaderPrivate
{
	/* The start byte position to read wal */
	XLogRecPtr	startptr;

	/* The end byte position to read wal */
	XLogRecPtr	endptr;

	/* The timeline id where we start to read wal */
	TimeLineID	timeline_id;

	/*
	 * The current byte position to read wal.
	 * curptr is updated in reading wal.
	 */
	XLogRecPtr	curptr;

	/*
	 * The next byte position passed XLogReaderState to read wal.
	 * When staring, nextptr point to startptr.
	 * After that, nextprt is set to InvalidXLogRecPtr.
	 */
	XLogRecPtr	nextptr;

	/*
	 * File descriptor of current reading segment file.
	 */
	int			curseg_file;

	/* The flag indicating whether wal reading is complete */
	bool		endptr_reached;

	/* The directory where we read wal */
	char		*waldir;

} WalReaderPrivate;

/*
 * Private data used in SQL functions.
 */
typedef struct WalReaderContext
{
	XLogReaderState  *xlogreader_state;

	/* Private context passed to XLogReaderState */
	WalReaderPrivate *private;

	/* Number of records read */
	uint32			 readnum;

} WalReaderContext;

/* Wal segment size */
static int wal_segment_size = DEFAULT_XLOG_SEG_SIZE;

/* Current TimeLine ID */
extern TimeLineID ThisTimeLineID;

/* Initialize wal reader private struct */
#define INIT_WALREADER_PRIVATE(private) \
	do { \
		private->startptr = InvalidXLogRecPtr; \
		private->endptr = InvalidXLogRecPtr; \
		private->timeline_id = 1; \
		private->curptr = InvalidXLogRecPtr; \
		private->nextptr = InvalidXLogRecPtr; \
		private->curseg_file = -1; \
		private->endptr_reached = false; \
		private->waldir = walreader_default_wal_directory; \
	} while (0)

/* Number of column which walreader returns */
#define MAX_ATTRS_NUM      13

#define RecPtrToLSN(recptr) \
	psprintf("%X/%08X", (uint32) (recptr >> 32), (uint32) recptr)

/* Indicate file descriptor is invalid */
#define InvalidFileHandle          -1

/* Code returned when an error occured in XLogReaderState */
#define XLogReaderReadPageError    -1

/*
 * This function sets the start byte position and the end byte position
 * to read xlog records.
 * 
 * This function receives three arguments.
 *  - private context
 *  - start lsn or segment file
 *  - end lsn or segment file
 *  - wal directory
 */
typedef void (*setup_walreader)(WalReaderPrivate *, char*, char*, char*);

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(read_wal_segment);
PG_FUNCTION_INFO_V1(read_wal_lsn);

void _PG_init(void);
void _PG_fini(void);

/*
 * Functions for wal reading
 */
static void ready_for_wal_read(PG_FUNCTION_ARGS, setup_walreader setup_func);
static void ready_for_reading_wal_segment(WalReaderPrivate *private,
										  char *start_walseg_file,
										  char *end_walseg_file,
										  char *walseg_directory);
static void ready_for_reading_wal_lsn(WalReaderPrivate *private,
										  char *start_wal_lsn,
										  char *end_wal_lsn,
										  char *walseg_directory);
static HeapTuple read_xlog_records(FuncCallContext *funcctx);
static HeapTuple make_tuple_xlog_record(FuncCallContext *funcctx);

static TupleDesc walreader_tupdesc();

static int WalReaderReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr,
							 int reqLen, XLogRecPtr targetPtr, char *readBuf,
							 TimeLineID *curFileTLI);
static int WalReaderXLogRead(WalReaderPrivate *private, XLogSegNo segno,
							 uint32 segoffm, char *readBuf, int count);
static void WalReaderRecordLen(XLogReaderState *record, uint32 *rec_len, uint32 *fpi_len);


/*
 * Helper functions
 */
static int open_file_in_directory(const char *directory, const char *fname);
static bool verify_directory(const char *directory);



/*
 * Module load callback
 */
void
_PG_init(void)
{
	/* Define custom GUC variables */
	DefineCustomStringVariable("walreader.default_wal_directory",
							   "Default directory to read wal from",
							   NULL,
							   &walreader_default_wal_directory,
							   XLOGDIR,
							   PGC_USERSET,
							   0,
							   NULL, NULL, NULL);

	DefineCustomIntVariable("walreader.read_limit",
							"Maximum number of reading wal",
							NULL,
							&walreader_read_limit,
							0,
							0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL, NULL, NULL);

}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks. */
}


/*
 * Read xlog records and returns set of record.
 * This sql function can receive start/end file names of wal segment and
 * the wal directory.
 */
Datum
read_wal_segment(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	Datum			 result;
	WalReaderContext *mycxt;
	WalReaderPrivate *private;
	/*
	 * Setup walreader and function context.
	 */
	if (SRF_IS_FIRSTCALL())
		ready_for_wal_read(fcinfo, ready_for_reading_wal_segment);

	/*
	 * Get a built context.
	 */
	funcctx = SRF_PERCALL_SETUP();
	mycxt = funcctx->user_fctx;
	private = mycxt->private;

	/*
	 * Read xlog records from startptr to endptr.
	 */
	HeapTuple tuple = read_xlog_records(funcctx);
	if (tuple != NULL && !private->endptr_reached)
	{
		result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * Read xlog records and returns set of record.
 * This sql function can receive start/end lsn of wal segment and the wal directory.
 */
Datum
read_wal_lsn(PG_FUNCTION_ARGS)
{
	FuncCallContext *funcctx;
	Datum			 result;
	WalReaderContext *mycxt;
	WalReaderPrivate *private;

	/*
	 * Setup walreader and function context.
	 */
	if (SRF_IS_FIRSTCALL())
		ready_for_wal_read(fcinfo, ready_for_reading_wal_lsn);

	/*
	 * Get a built context.
	 */
	funcctx = SRF_PERCALL_SETUP();
	mycxt = funcctx->user_fctx;
	private = mycxt->private;

	/*
	 * Read xlog records from startptr to endptr.
	 */
	HeapTuple tuple = read_xlog_records(funcctx);
	if (tuple != NULL && !private->endptr_reached)
	{
		result = HeapTupleGetDatum(tuple);
		SRF_RETURN_NEXT(funcctx, result);
	}

	SRF_RETURN_DONE(funcctx);
}

/*
 * Ready for wal reading.
 *
 * This function must be called in SRF first call.
 * Setup FuncCallContext and XLogReaderState objects.
 */
static void
ready_for_wal_read(PG_FUNCTION_ARGS, setup_walreader setup_func)
{
	FuncCallContext 	*funcctx;
	MemoryContext		oldcontext;
	AttInMetadata 		*attinmeta;
	TupleDesc			tupdesc;
	WalReaderPrivate	*private;
	WalReaderContext	*mycxt;
	XLogReaderState		*xlogreader_state;
	char				*start_wal = NULL;
	char				*end_wal = NULL;
	char				*waldir = NULL;

	int argn = PG_NARGS();
	int i = 0;

	/*
	 * Setup FuncCallContext.
	 */
	funcctx = SRF_FIRSTCALL_INIT();
	oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

	start_wal = text_to_cstring(PG_GETARG_TEXT_PP(i));

	if (++i < argn)
		end_wal = text_to_cstring(PG_GETARG_TEXT_PP(i));

	if (++i < argn)
		waldir = text_to_cstring(PG_GETARG_TEXT_PP(i));


	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("function returning record called in context "
						"that cannot accept type record")));

	/*
	* Initialize private state.
	*/
	private = palloc(sizeof(WalReaderPrivate));
	INIT_WALREADER_PRIVATE(private);

	/*
	* Ready for reading wal records with the specified arguments.
	*/
	setup_func(private, start_wal, end_wal, waldir);
	private->curptr = private->nextptr = private->startptr;

	xlogreader_state = XLogReaderAllocate(wal_segment_size, WalReaderReadPage,
										  private);
	if (xlogreader_state == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Could not allocate enough memory to read wal.")));

	/*
	 * Construct the context for walreader while SFR is running.
	 */
	mycxt = palloc(sizeof(WalReaderContext));
	mycxt->xlogreader_state = xlogreader_state; 
	mycxt->private = private;
	mycxt->readnum = 0;

	/*
	 * Construct tuple descriptor.
	 */
	tupdesc = walreader_tupdesc();

	funcctx->user_fctx = mycxt;
	attinmeta = TupleDescGetAttInMetadata(tupdesc);
	funcctx->attinmeta = attinmeta;

	MemoryContextSwitchTo(oldcontext);
}


/*
 * Ready for reading wal with segment file.
 */
static void
ready_for_reading_wal_segment(WalReaderPrivate *private, 
							  char *start_walseg_file, char *end_walseg_file,
							  char *walseg_directory)
{
	XLogSegNo		segno;
	TimeLineID		timeline_id;

	/*
	 * Define record starting position.
	 */
	XLogFromFileName(start_walseg_file, &timeline_id, &segno, wal_segment_size);
	if (segno <= 0)
		elog(ERROR, "start_walseg_file is invalid: %s", start_walseg_file);

	XLogSegNoOffsetToRecPtr(segno, 0, wal_segment_size, private->startptr);

	/* This is the timeline we start to read xlog records */
	private->timeline_id = timeline_id;

	/*
	 * Define record end position if end_segment_file exists.
	 */
	if (end_walseg_file)
	{
		XLogFromFileName(end_walseg_file, &timeline_id, &segno, wal_segment_size);
		if (segno <= 0)
			elog(ERROR, "end_walseg_file is invalid: %s", end_walseg_file);

		XLogSegNoOffsetToRecPtr(segno + 1, 0, wal_segment_size, private->endptr);
	}

	/*
	 * If endptr is invalid, we set it to the starting position of next segment.
	 * 
	 */
	if (XLogRecPtrIsInvalid(private->endptr))
		XLogSegNoOffsetToRecPtr(segno + 1, 0, wal_segment_size, private->endptr);

	/*
	 * If walseg_directory is passed, we verify the directory exsists.
	 */
	if (walseg_directory != NULL)
	{
		if (!verify_directory(walseg_directory))
			ereport(ERROR,
					(errcode_for_file_access(),
					errmsg("could not stat directory \"%s\": %m",
							walseg_directory)));
		else
			private->waldir = walseg_directory;
	}
}

/*
 * Ready for reading wal with wal lsn.
 */
static void
ready_for_reading_wal_lsn(WalReaderPrivate *private, 
						  char *start_lsn, char *end_lsn,
						  char *walseg_directory)
{
	XLogSegNo		segno;
	uint32			xlogid;
	uint32			xrecoff;

	if (sscanf(start_lsn, "%X/%X", &xlogid, &xrecoff) != 2)
		elog(ERROR, "could not parse start WAL location \"%s\"", start_lsn);

	/*
	 * Define record starting position.
	 */
	private->startptr = (uint64) xlogid << 32 | xrecoff;

	/* 
	 * We always set start timeline to current timeline.
	 */
	private->timeline_id = ThisTimeLineID;

	/*
	 * Define record end position if end_segment_file exists.
	 */
	if (end_lsn)
	{
		if (sscanf(end_lsn, "%X/%X", &xlogid, &xrecoff) != 2)
			elog(ERROR, "could not parse end WAL location \"%s\"", end_lsn);
	
		private->endptr = (uint64) xlogid << 32 | xrecoff;
	}

	/*
	 * If endptr is invalid, we set it to the starting position of next segment.
	 * 
	 */
	if (XLogRecPtrIsInvalid(private->endptr))
	{
		XLByteToSeg(private->startptr, segno, wal_segment_size);
		XLogSegNoOffsetToRecPtr(segno + 1, 0, wal_segment_size, private->endptr);
	}

	/*
	 * If walseg_directory is passed, we verify the directory exsists.
	 */
	if (walseg_directory != NULL)
	{
		if (!verify_directory(walseg_directory))
			ereport(ERROR,
					(errcode_for_file_access(),
					errmsg("could not stat directory \"%s\": %m",
							walseg_directory)));
		else
			private->waldir = walseg_directory;
	}
}



/*
 * Read xlog records from the specified startptr and endptr.
 *
 * Returns null if there is no record to read.
 */
static HeapTuple read_xlog_records(FuncCallContext *funcctx)
{
	XLogReaderState  *xlogreader_state;
	WalReaderContext *mycxt;
	WalReaderPrivate *private;
	XLogRecord		 *record;
	XLogRecPtr		 first_record;
	HeapTuple		 tuple;
	char			 *errormsg;
	MemoryContext	 oldcontext;

	Assert(funcctx->user_fctx != NULL);

	mycxt = funcctx->user_fctx;
	private = mycxt->private;


	/*
	 * Over limit, so we donot read wad record anymore.
	 */
	if (walreader_read_limit > 0 && mycxt->readnum >= walreader_read_limit)
		goto stop_reading;

	/*
	 * When we continue reading xlog records in the same segment file,
	 * nextptr will points to InvalidXLogRecPtr.
	 */
	first_record = private->nextptr;

	/*
	 * If we're at a page boundary, points to a valid record starting position.
	 */
	if (!XLogRecPtrIsInvalid(first_record))
	{
		if (first_record % XLOG_BLCKSZ == 0)
		{
			if (XLogSegmentOffset(first_record, wal_segment_size) == 0)
				first_record += SizeOfXLogLongPHD;
			else
				first_record += SizeOfXLogShortPHD;
		}
	}

	xlogreader_state = mycxt->xlogreader_state;

	/*
	 * We need to switch to multi_call_memory_ctx in reading xlog.
	 */
	oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

	for (;;)
	{
		/* Try to read the next record */
		record = XLogReadRecord(xlogreader_state, first_record, &errormsg);

		MemoryContextSwitchTo(oldcontext);

		if (!record)
		{
			if (errormsg)
				elog(WARNING, "%s", errormsg);

			private->endptr_reached = true;

			break;
		}

		/* After reading the first record, continue at next one */
		private->nextptr = InvalidXLogRecPtr;

		/* Count up number of records read */
		mycxt->readnum++;

		/*
		 * OK, found a record!
		 * Make a tuple.
		 */
		tuple = make_tuple_xlog_record(funcctx);

		return tuple;
	}

stop_reading:

	/*
	 * Close the opened segment file if any.
	 */
	if (private->curseg_file >= 0)
		close(private->curseg_file);

	XLogReaderFree(mycxt->xlogreader_state);

	/* Done. */
	return NULL;
}

/*
 * Make a tuple filled with xlog record.
 */
static HeapTuple
make_tuple_xlog_record(FuncCallContext *funcctx)
{
	AttInMetadata	 *attinmeta;
	const char 		 *id;
	const RmgrData	 *desc;
	uint32			 rec_len;
	uint32			 fpi_len;
	uint8			 info;
	XLogRecPtr		 xl_prev;
	XLogReaderState  *xlogreader_state;
	WalReaderContext *mycxt;
	WalReaderPrivate *private;
	HeapTuple		 tuple;
	char			 filename[MAXPGPATH];
	char			 *values[MAX_ATTRS_NUM];
	int				 colno;

	Assert(funcctx->user_fctx != NULL);

	mycxt = funcctx->user_fctx;
	attinmeta = funcctx->attinmeta;

	xlogreader_state = mycxt->xlogreader_state;
	private = mycxt->private;

	/*
	 * Get rmgr definition from current deceded xlog record.
	 */
	desc = &RmgrTable[XLogRecGetRmid(xlogreader_state)];

	info = XLogRecGetInfo(xlogreader_state);
	xl_prev = XLogRecGetPrev(xlogreader_state);

	id = desc->rm_identify(info);
	if (id == NULL)
		id = psprintf("UNKNOWN (%x)", info & ~XLR_INFO_MASK);

	WalReaderRecordLen(xlogreader_state, &rec_len, &fpi_len);

	colno = 0;

	/* Timeline ID */
	values[colno++] = psprintf("%u", private->timeline_id);

	/* Wal segment file */
	XLogFileName(filename, private->timeline_id, xlogreader_state->readSegNo,
				 wal_segment_size);
	values[colno++] = pstrdup(filename);

	/* segment offset */
	uint32 startoff = XLogSegmentOffset(xlogreader_state->ReadRecPtr, wal_segment_size);
	values[colno++] = psprintf("%u", startoff);

	/* page in the segment */
	values[colno++] = psprintf("%u", (xlogreader_state->readOff / XLOG_BLCKSZ) + 1);

	/* page offset */
	values[colno++] = psprintf("%u", (int)(xlogreader_state->ReadRecPtr % XLOG_BLCKSZ));

	/* rmgr name */
	values[colno++] = pstrdup(desc->rm_name);

	/* rec_len */
	values[colno++] = psprintf("%u", rec_len);

	/* tot_len */
	values[colno++] = psprintf("%u", XLogRecGetTotalLen(xlogreader_state));

	/* txid */
	values[colno++] = psprintf("%u", XLogRecGetXid(xlogreader_state));

	/* lsn */
	values[colno++] = RecPtrToLSN(xlogreader_state->ReadRecPtr);

	/* prev_lsn */
	values[colno++] = RecPtrToLSN(xl_prev);

	/* identify */
	values[colno++] = pstrdup(id);

	/* rmgr desc */
	StringInfoData buf;
	initStringInfo(&buf);
	desc->rm_desc(&buf, xlogreader_state);
	values[colno++] = pstrdup(buf.data);
	pfree(buf.data);

	tuple = BuildTupleFromCStrings(attinmeta, values);

	return tuple;
}


/*
 * Construct a tuple descriptor for walreader and return it.
 */
static TupleDesc
walreader_tupdesc()
{
	TupleDesc	tupdesc;
	AttrNumber	attr_num = 0;

	tupdesc = CreateTemplateTupleDesc(MAX_ATTRS_NUM);

	TupleDescInitEntry(tupdesc, (AttrNumber) ++attr_num, "timeline",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) ++attr_num, "walseg",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) ++attr_num, "seg_off",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) ++attr_num, "page",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) ++attr_num, "page_off",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) ++attr_num, "rmgr",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) ++attr_num, "rec_len",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) ++attr_num, "tot_len",
					   INT4OID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) ++attr_num, "tx",
					   XIDOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) ++attr_num, "lsn",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) ++attr_num, "prev_lsn",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) ++attr_num, "identify",
					   TEXTOID, -1, 0);
	TupleDescInitEntry(tupdesc, (AttrNumber) ++attr_num, "rmgr_desc",
					   TEXTOID, -1, 0);

	Assert(attr_num <= MAX_ATTRS_NUM);

	return BlessTupleDesc(tupdesc);
}


/*
 * Callback function invoked in ReadPageInternal() in xlogreader.c.
 */
static int
WalReaderReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				  XLogRecPtr targetPtr, char *readBuf, TimeLineID *curFileTLI)
{
#define XLOG_OPEN_MAX_RETRIES     5
#define XLOG_OPEN_WAIT_MSEC       500 * 1000

	/* The reading segment number */
	XLogSegNo	segno  = state->readSegNo;
	uint32		segoff = state->readOff;

	WalReaderPrivate *private = state->private_data;
	int count = XLOG_BLCKSZ;
	int readbyte = 0;

	/*
	 * Get the size of data to read next.
	 */
	if (private->endptr != InvalidXLogRecPtr)
	{
		if (targetPagePtr + XLOG_BLCKSZ <= private->endptr)
			count = XLOG_BLCKSZ;
		else if (targetPagePtr + reqLen <= private->endptr)
			count = private->endptr - targetPagePtr;
		else
		{
			private->endptr_reached = true;
			/*
			 * Returning -1, XlogReader wll stop to read.
			 */
			return XLogReaderReadPageError;
		}
	}

	if (segoff == 0)
		private->curptr = targetPagePtr;

	/*
	 * Need to switch a new segment file?
	 */
	if (private->curseg_file < 0 ||
		!XLByteInSeg(private->curptr, segno, wal_segment_size))
	{
		char	filename[MAXFNAMELEN];
		int		tries;
		int		segfile;

		/* Switch to another logfile segment */
		if (private->curseg_file >= 0)
		{
			close(private->curseg_file);
			private->curseg_file = InvalidFileHandle;

			/*
			 * nextptr needs to be initialized per segment file.
			 * Now segment file has been switched, we point nextptr to curptr.
			 * curptr should be pointed to the beginning of the segment file.
			 */
			private->nextptr = private->curptr;
		}

		/*
		 * Get the name of the wal file to open.
		 */
		XLByteToSeg(private->curptr, segno, wal_segment_size);
		XLogFileName(filename, private->timeline_id, segno, wal_segment_size);

		/*
		 * In follow mode there is a short period of time after the server
		 * has written the end of the previous file before the new file is
		 * available. So we loop for 5 seconds looking for the file to
		 * appear before giving up.
		 */
		for (tries = 0; tries < XLOG_OPEN_MAX_RETRIES; tries++)
		{
			segfile = open_file_in_directory(private->waldir, filename);
			if (segfile >= 0)
				break;

			if (errno == EINTR)
				continue;

			/* Any other error, fall through and fail */
			break;
		}

		if (segfile < 0)
		{
			ereport(WARNING,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", filename)));

			return XLogReaderReadPageError;
		}

		/*
		 * Set next segment file to read.
		 */
		private->curseg_file = segfile;
	}

	/*
	 * Read count bytes of data from the position of the target page.
	 */
	readbyte = WalReaderXLogRead(private, segno, segoff, readBuf, count);

	return readbyte;
}

/*
 * Read data 
 *
 */
static int
WalReaderXLogRead(WalReaderPrivate *private, XLogSegNo segno, uint32 segoff,
				  char *readBuf, int count)
{
	char	*bufpos;
	int		leftbytes;

	bufpos = readBuf;
	leftbytes = count;

	while (leftbytes > 0)
	{
		uint32		startoff;
		int			segbytes;
		int			readbytes;
		int 		segfile = private->curseg_file;

		startoff = XLogSegmentOffset(private->curptr, wal_segment_size);

		/* Need to seek in the file? */
		if (segoff != startoff)
		{
			if (lseek(segfile, (off_t) startoff, SEEK_SET) < 0)
			{
				int		err = errno;
				char	filename[MAXPGPATH];

				XLogFileName(filename, private->timeline_id, segno, wal_segment_size);

				elog(WARNING, "could not seek in log file %s to offset %u: %s",
							filename, startoff, strerror(err));

				goto handle_error;
			}
		}

		/* How many bytes are within this segment? */
		if (leftbytes > (wal_segment_size - startoff))
			segbytes = wal_segment_size - startoff;
		else
			segbytes = leftbytes;

		do {
			readbytes = read(segfile, bufpos, segbytes);
		} while (readbytes < 0 && errno == EINTR);

		if (readbytes <= 0)
		{
			int		err = errno;
			char	filename[MAXPGPATH];
			int		save_errno = errno;

			XLogFileName(filename, private->timeline_id, segno, wal_segment_size);
			errno = save_errno;

			if (readbytes < 0)
				elog(WARNING, "could not read from log file %s, offset %u, length %d: %s",
							filename, segoff, segbytes, strerror(err));
			else if (readbytes == 0)
				elog(WARNING, "could not read from log file %s, offset %u: read %d of %zu",
							filename, segoff, readbytes, (Size) segbytes);

			goto handle_error;
		}

		/* Update state for read */
		private->curptr += readbytes;

		segoff += readbytes;
		bufpos += readbytes;
		leftbytes -= readbytes;
	}

	return count;


handle_error:

	return XLogReaderReadPageError;
}

/*
 * Calculate the size of a record.
 */
static void
WalReaderRecordLen(XLogReaderState *record, uint32 *rec_len, uint32 *fpi_len)
{
	int			block_id;

	*fpi_len = 0;
	for (block_id = 0; block_id <= record->max_block_id; block_id++)
	{
		if (XLogRecHasBlockImage(record, block_id))
			*fpi_len += record->blocks[block_id].bimg_len;
	}

	/*
	 * Calculate the length of the record as the total length - the length of
	 * all the block images.
	 */
	*rec_len = XLogRecGetTotalLen(record) - *fpi_len;
}


/*
 * Open a file and return the file descriptor.
 */
static int
open_file_in_directory(const char *directory, const char *fname)
{
	int		fd = InvalidFileHandle;
	char	fpath[MAXPGPATH];

	Assert(directory != NULL && fname != NULL);

	snprintf(fpath, MAXPGPATH, "%s/%s", directory, fname);
	fd = BasicOpenFile(fpath, O_RDONLY | PG_BINARY);

	if (fd < 0 && errno != ENOENT)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", directory)));

	return fd;
}

/*
 * Verify whether the specified directory exists.
 * This function will try to open the directory and close it.
 */
static bool verify_directory(const char *directory)
{
	DIR *dir;

	if ((dir = AllocateDir(directory)) == NULL)
		return false;

	FreeDir(dir);
	return true;
}
