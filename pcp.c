/* for strdupa */
#define _GNU_SOURCE

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <syscall.h>
#include <time.h>
#include <errno.h>
#include "pcp.h"

#define PATH_LEN_MAX 256
#define THREADS_NUM_MAX 256
#define SYS_THREADS_NUM_MAX 4096
#define SYS_THREADS_NUM_DEF 64
#define THREADS_NUM_DEF 16
#define BLOCK_SIZE_MAX  (1*1024*1024)
#define MNT_POINTS_MAX 16
#define BLOCK_SIZE_MIN 1
#define BLOCK_SIZE_DEF (64*1024)

char *__progname;

int __log_level = LOG_ERROR;

typedef struct PathParams {
    const char *dstpath;
    const char *srcpath;
    int srcfd;
    int dstfd;
} PathParams;

typedef struct BufIO {
    void *buf;
    size_t len;
    off_t offset;
    struct BufIO *next;
    struct BufIO *prev;
} BufIO;

typedef struct BufQueue {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    BufIO queue;
    off_t offset;
    int exit;
} BufQueue;

typedef struct CpParams {
    const char *srcpath[MNT_POINTS_MAX];
    const char *dstpath[MNT_POINTS_MAX];
    int src_fds[MNT_POINTS_MAX];
    int dst_fds[MNT_POINTS_MAX];
    volatile off_t *shared_offset;
    int handle_sparse;
    int sync_mode;
    PathParams paths[MNT_POINTS_MAX * MNT_POINTS_MAX];
    int n_srcs;
    int n_dsts;

    int src_stream;
    int dst_stream;
    BufQueue stream_io;
    BufQueue stream_free;

    int force;					/* if file exists, force overriding it */
	int async;					/* move program to background */
	char *sys_md_file;		    /* file used to sync several async/background pcp. See also sys_max_threads */
	int sys_max_threads;		/* max concurrent async/background sys threads */
    int tarmode;				/* assume tar is forking us as --to-command filter, no args are assumed and all file args are read from env */
    int first_path_create_only;
    mode_t mode;
    int threads;
    int blocksize;
    unsigned long length;

    char *logfile_path;
    FILE *logfile;
    struct tm *starttime;
    struct tm *endtime;
    off_t written_bytes;

	TarFile *tarparams;
} CpParams;

typedef struct PcpThreadArgs {
    PathParams *paths;
    CpParams *cp;
} PcpThreadArgs;

#define xstr(s) str(s)
#define str(s) #s
const char *commit = xstr(COMMIT);

const char __zerobuf[BLOCK_SIZE_MAX];

void usage()
{
    fprintf(stderr,
            "Usage: %s [-b <blocksize> -m <octal mode> -f -d -s] <src file> <dest file>\n",
            __progname);
    fprintf(stderr,
            "Usage: %s [-b <blocksize> -m <octal mode> -f -d -s] <src file1> <src file2> < src file-n> -- <dest file1> <dest file2> <dest file n>\n",
            __progname);
    fprintf(stderr, "Usage: %s -T \n", __progname);
    fprintf(stderr, "\n");
    fprintf(stderr,
            "\t-h			Print the usage screen (this screen).\n");
    fprintf(stderr,
            "\t-b <blocksize>		IO blocksize to use [ %d - %d ], prefixes bKMGT are supported. Default %d.\n",
            BLOCK_SIZE_MIN, BLOCK_SIZE_MAX, BLOCK_SIZE_DEF);
    fprintf(stderr,
            "\t-l <length>		Max byts to read from source. prefixes bKMGT are supported. Default 0 = all file.\n");
    fprintf(stderr,
            "\t-d			Increase debug level. Default %d.\n",
            __log_level);
    fprintf(stderr,
            "\t-f			Force - allow destination file overwrite. Default - fail if destination file exists.\n");
    fprintf(stderr,
            "\t-m <octal mode>		destination file mode if created. Default <source> mode.\n");
    fprintf(stderr,
            "\t-t <threads_num>	Number of threads to use [1-%d]. Default %d.\n",
            THREADS_NUM_MAX, THREADS_NUM_DEF);
    fprintf(stderr,
            "\t-S			Sparse file support. Default NO.\n");
    fprintf(stderr,
            "\t-s			Sync IO mode. Default async.\n");
    fprintf(stderr,
            "\t-v			Print version and exit.\n");
    fprintf(stderr,
            "\t-C			Create/Open the file on first mount, but do the IOs on other mounts. Relevant only if -M is used. Default NO.\n");
    fprintf(stderr,
            "\t-L <logfile>		Log the operation in the specified file, mimicking gsutil cp -L.\n");
    fprintf(stderr,
            "\t-T			Tar mode. No args are provides, real args are read from env (see tar --to-command section)\n");
    fprintf(stderr,
            "\t-X <threads>		Max system threads. This is set by the first process to create the sys md file (see -M). Default: %d\n", SYS_THREADS_NUM_DEF);
    fprintf(stderr,
            "\t-M <mdfile> 		System MD file. This file is used to coordinate the concurrent running threads.\n"\
			"\t\t\t\tThe first process creates it and the other attach them self to it.\n");
    fprintf(stderr,
            "\t-P <mdfile> 		Peek into a system MD file - show the free threads and exit.\n");
    fprintf(stderr,
            "\t-a			Async (background) mode. The process moves itself to background before doing IO.\n");
    fprintf(stderr, "\n");
    exit(1);
}

CpParams *error_params;

/**
 * root@32ed39fd7323:/opt/bnt# cat nlog2.log
Source,Destination,Start,End,Md5,UploadId,Source Size,Bytes Transferred,Result,Description
file://-,file://t.txt,2018-09-17T05:47:10.565766Z,2018-09-17T05:47:17.708018Z,,,,55854791,OK,
root@32ed39fd7323:/opt/bnt#
*/
void logfile(CpParams * params, char *error)
{
    void panic(int err, char *msg, ...);
    void debug(int level, char *msg, ...);

    char start[100] = "", end[100] = "";
    time_t t;
    struct tm *tm;

    if (params->logfile == NULL) {
        return;
    }
    t = time(NULL);
    tm = localtime(&t);
    if (tm == NULL) {
        error_params = NULL;
        panic(1, "can't get time stamp");
    }

    if ((params->starttime
         && strftime(start, sizeof(start) - 1, "%FT%T.0",
                     params->starttime) < 0) || (params->endtime
                                                 && strftime(end,
                                                             sizeof(end) -
                                                             1, "%FT%T.0",
                                                             params->
                                                             endtime)
                                                 < 0)) {
        error_params = NULL;
        panic(1, "can't create time strings");
    }

    debug(LOG_DEBUG, "emit log entry to file '%s'", params->logfile_path);
    fprintf(params->logfile, "\"%s\",\"%s\",%s,%s,,,,%lu,%s,\"%s\"\n",
            params->srcpath[0],
            params->dstpath[0],
            start,
            end,
            (ulong) params->written_bytes,
            error ? "ERROR" : "OK", error ? error : "");
}

void panic(int err, char *msg, ...)
{
    va_list ap;
    char buf[8192];

    va_start(ap, msg);
    vsnprintf(buf, (sizeof buf) - 1, msg, ap);
    buf[(sizeof buf) - 1] = 0;
    va_end(ap);

    if (err) {
        fprintf(stderr, "Panic: %s: %s [ %m ]\n", __progname, buf);
    } else {
        fprintf(stderr, "Panic: %s: %s\n", __progname, buf);
    }

    if (error_params) {
        logfile(error_params, buf);
    }
    exit(1);
}

pid_t gettid()
{
    return syscall(SYS_gettid);
}

int format_time(char *buf, uint len)
{
    int ret;
    struct tm t;
    struct timespec ts;

    clock_gettime(CLOCK_REALTIME, &ts);

    tzset();
    if (localtime_r(&(ts.tv_sec), &t) == NULL) {
        return 1;
    }

    ret = strftime(buf, len, "%F %T", &t);
    if (ret == 0) {
        return 2;
    }
    len -= ret - 1;

    ret = snprintf(&buf[strlen(buf)], len, ".%09ld", ts.tv_nsec);
    if (ret >= len) {
        return 3;
    }

    return 0;
}

void debug(int level, char *msg, ...)
{
    va_list ap;
    char buf[8192];
    char now[64];

    if (level > __log_level) {
        return;
    }

    format_time(now, (sizeof now) - 1);

    va_start(ap, msg);
    vsnprintf(buf, (sizeof buf) - 1, msg, ap);
    buf[(sizeof buf) - 1] = 0;
    va_end(ap);

    fprintf(stderr, "%s %s[%d]: %s\n", now, __progname, gettid(), buf);
}

void _qput(BufQueue * q, BufIO * buf, int ordered)
{
    BufIO *p;

    pthread_mutex_lock(&q->mutex);
    debug(LOG_VERBOSE,
          "_qput: q %p buf %p exit %d ordered %d q offset %ld", q, buf,
          q->exit, ordered, (long) q->offset);
    if (q->exit) {
        panic(0, "can't use queue %p after exit is signalled!", q);
        pthread_mutex_unlock(&q->mutex);
    }
    p = q->queue.next;
    while (p != &q->queue && (ordered && p->offset > buf->offset)) {
        debug(LOG_VERBOSE, "_qput: q %p buf offset %ld skip %ld (%p)", q,
              (long) buf->offset, p->offset, p);
        p = p->next;
    }
    debug(LOG_VERBOSE, "_qput: q %p insert buf offset %ld before %ld (%p)",
          q, (long) buf->offset, p->offset, p);
    buf->prev = p->prev;
    p->prev->next = buf;
    buf->next = p;
    p->prev = buf;

    if (!ordered || buf->offset == q->offset) {
        debug(LOG_VERBOSE,
              "_qput: q %p buf %p ordered %d q offset %ld SIGNAL", q, buf,
              ordered, (long) q->offset);
        pthread_cond_signal(&q->cond);
    }
    pthread_mutex_unlock(&q->mutex);
}

void qput(BufQueue * q, BufIO * buf)
{
    _qput(q, buf, 0);
}

void qput_ordered(BufQueue * q, BufIO * buf)
{
    _qput(q, buf, 1);
}

BufIO *_qget(BufQueue * q, int ordered)
{
    BufIO *buf;

    pthread_mutex_lock(&q->mutex);
    debug(LOG_VERBOSE,
          "_qget: q %p exit %d ordered %d q prev %p offset %ld prev offset %ld",
          q, q->exit, ordered, q->queue.prev, q->offset,
          q->queue.prev->offset);
    while (!q->exit &&          /* if exit is signalled we shouldn't wait - just drain the queue and exit */
           (q->queue.prev == &q->queue  /* queue is empty */
            || (ordered && q->queue.prev->offset != q->offset)  /* or in ordered mode - next entry is not in order */
           )) {
        debug(LOG_VERBOSE,
              "_qget: WAIT: q %p exit %d ordered %d q prev %p offset %ld prev offset %ld",
              q, q->exit, ordered, q->queue.prev, q->offset,
              q->queue.prev->offset);
        pthread_cond_wait(&q->cond, &q->mutex);
    }
    /* even if exit is signaled, let the queue to be drained. Qput ensures that no more entries can be inserted. */
    if (q->exit && q->queue.prev == &q->queue) {
        debug(LOG_VERBOSE, "_qget: q %p is in exit mode", q);
        pthread_mutex_unlock(&q->mutex);
        return NULL;
    }

    buf = q->queue.prev;
    buf->next->prev = buf->prev;
    buf->prev->next = buf->next;
    q->offset += buf->len;
    pthread_mutex_unlock(&q->mutex);
    return buf;
}

BufIO *qget(BufQueue * q)
{
    return _qget(q, 0);
}

BufIO *qget_ordered(BufQueue * q)
{
    return _qget(q, 1);
}

void qinit(BufQueue * q, uint items, uint bufsize)
{
    BufIO *ios = calloc(items, sizeof(BufIO));
    void *bufs = calloc(items, bufsize);

    q->queue.next = q->queue.prev = &q->queue;
    q->exit = 0;
    q->offset = 0;
    pthread_mutex_init(&q->mutex, NULL);
    pthread_cond_init(&q->cond, NULL);

    BufIO *io;
    if (ios == NULL || bufs == NULL) {
        panic(0,
              "can't alloc array of %d buf items, each with buffer size %d",
              items, bufsize);
    }
    for (io = ios; io < ios + items; io++) {
        io->buf = bufs + (io - ios) * bufsize;
        qput(q, io);
    }
}

/**
 * Signal exit condition for all queue waiters.
 * @param q
 */
void qexit(BufQueue * q)
{
    pthread_mutex_lock(&q->mutex);
    q->exit = 1;
    debug(LOG_DEBUG, "qexit: Signal EXIT on q %p", q);
    pthread_cond_broadcast(&q->cond);
    pthread_mutex_unlock(&q->mutex);
}

/**
 * atomically increment 'offset' by one and return previous value
 */
inline off_t atomic_fetch_and_add_offset(volatile off_t * offset,
                                         off_t value)
{
    /* atomically incref and return previous value */
    return __sync_fetch_and_add(offset, value);
}

/**
 * Single thread to read stream and push IO requests to IO-Q.
 * @param v     PcpThreadArgs
 * @return      NULL
 */
void *pcp_stream_reader(void *v)
{
    PcpThreadArgs *args = (PcpThreadArgs *) v;
    size_t blocksize = args->cp->blocksize;
    int srcfd = args->paths->srcfd;

    ssize_t count;
    off_t offset = 0;
    BufIO *io;
    BufQueue *freeq = &args->cp->stream_free;
    BufQueue *ioq = &args->cp->stream_io;
    int bytes;

    do {
        if (!(io = qget(freeq))) {
            debug(LOG_DEBUG,
                  "pcp_stream_reader got signal EXIT on free q");
            break;              /* Exit signaled on queue */
        }

        count = 0;
        /* read a full block */
        do {
            bytes = read(srcfd, io->buf + count, blocksize - count);
            debug(LOG_DEBUG, "pcp_stream_reader %d bytes (offset %ld)",
                  bytes, (long) offset);
            if (bytes <= 0) {
                debug(LOG_DEBUG, "pcp_stream_reader got EOF");
                break;          /* EOF */
            }
            count += bytes;
        } while (count < blocksize);

        if (count == 0) {
            /* Nothing in buffer, EOF or error */
            break;
        }
        io->len = count;
        io->offset = offset;
        debug(LOG_DEBUG,
              "pcp_stream_reader read offset %ld count %ld enqueue io %p",
              (long) io->offset, (long) io->len, io);
        offset += count;
        qput(ioq, io);
    } while (count >= 0);

    debug(LOG_DEBUG, "pcp_stream_reader signal EXIT");
    qexit(ioq);                 /* signal exit on IO q */

    return NULL;
}

void *pcp_stream_writer(void *v)
{
    PcpThreadArgs *args = (PcpThreadArgs *) v;
    int dstfd = args->paths->dstfd;

    ssize_t count;
    BufIO *io;
    BufQueue *freeq = &args->cp->stream_free;
    BufQueue *ioq = &args->cp->stream_io;
    int bytes;

    do {
        if (!(io = qget_ordered(ioq))) {
            debug(LOG_DEBUG, "pcp_stream_writer got signal EXIT on ioq q");
            break;              /* Exit signaled on queue */
        }

        debug(LOG_DEBUG,
              "pcp_stream_writer got offset %ld count %ld enqueue io %p",
              (long) io->offset, (long) io->len, io);
        count = 0;
        /* ensure that the entire writeio->len bytes are written */
        do {
            bytes = write(dstfd, io->buf + count, io->len - count);
            debug(LOG_DEBUG, "pcp_stream_writer %d bytes (offset %ld)",
                  bytes, (long) io->offset);
            if (bytes < 0) {
                debug(LOG_DEBUG, "pcp_stream_writer got error %m");
                break;          /* EOF */
            }
            count += bytes;
            atomic_fetch_and_add_offset(&args->cp->written_bytes, bytes);
        } while (count < io->len);

        if (count == 0) {
            /* Nothing in buffer, EOF or error */
            break;
        }
        debug(LOG_DEBUG, "pcp_stream_writer frees io %p", io);
        qput(freeq, io);
    } while (count >= 0);

    debug(LOG_DEBUG, "pcp_stream_writer DONE");

    return NULL;
}

void *pcp_cp_from_stream(void *v)
{
    PcpThreadArgs *args = (PcpThreadArgs *) v;
    int dstfd = args->paths->dstfd;
    BufQueue *freeq = &args->cp->stream_free;
    BufQueue *ioq = &args->cp->stream_io;
    BufIO *io;

    while ((io = qget(ioq))) {
        /* if handle sparse, check of read buffer is zero */
        if (io->len <= 0) {
            break;              /* EOF */
        }
        debug(LOG_DEBUG,
              "pcp_cp_from_stream io %p copies offset %ld count %ld", io,
              (long) io->offset, (long) io->len);
        if (!args->cp->handle_sparse
            || memcmp(io->buf, __zerobuf, io->len) != 0) {
            /* partial writes shouldn't happen in our case */
            if (pwrite(dstfd, io->buf, io->len, io->offset) != io->len) {
                panic(1,
                      "partial write on destination file '%s' offset %lu count %zu",
                      args->paths->dstpath, (ulong) io->offset, io->len);
            }
            atomic_fetch_and_add_offset(&args->cp->written_bytes, io->len);
        }
        debug(LOG_DEBUG, "pcp_cp_from_stream put %p to free q", io);
        qput(freeq, io);
    }

    debug(LOG_DEBUG, "pcp_cp_from_stream DONE");
    return NULL;
}

void *pcp_cp_to_stream(void *v)
{
    PcpThreadArgs *args = (PcpThreadArgs *) v;
    volatile off_t *shared_offset = args->cp->shared_offset;
    BufQueue *freeq = &args->cp->stream_free;
    BufQueue *ioq = &args->cp->stream_io;
    int blocksize = args->cp->blocksize;
    int srcfd = args->paths->srcfd;
    off_t offset;
    int count, bytes;
    BufIO *io;

    while ((io = qget(freeq))) {
        offset = atomic_fetch_and_add_offset(shared_offset, blocksize);
        debug(LOG_DEBUG, "pcp_cp_to_stream handle offset %ld count %d",
              (long) offset, (int) blocksize);

        size_t iosize = blocksize;

        if (args->cp->length) {
            if (offset >= args->cp->length) {
                debug(LOG_DEBUG, "pcp_cp_to_stream EOF");
                break;
            }
            if (offset + iosize > args->cp->length) {
                iosize = args->cp->length - offset;
            }
        }

        io->offset = offset;

        /* loop on read as read may return LESS than desired */
        for (count = 0, bytes = 0; count < iosize; count += bytes) {
            debug(LOG_DEBUG, "pcp_cp_to_stream read offset %ld count %d",
                  (long) offset + count, (int) iosize - count);

            bytes =
                pread(srcfd, io->buf + count, iosize - count,
                      io->offset + count);

            if (bytes <= 0) {
                debug(LOG_DEBUG, "pcp_cp_to_stream EOF/error on read");
                break;
            }
        }
        io->len = count;

        if (count) {
            debug(LOG_DEBUG, "pcp_cp_to_stream put offset %ld count %d",
                  (long) io->offset, (int) io->len);
            qput_ordered(ioq, io);
        }

        if (io->len < iosize)
            break;
    }

    debug(LOG_DEBUG, "pcp_cp_to_stream DONE");
    return NULL;
}

void *pcp_thread(void *v)
{
    PcpThreadArgs *args = (PcpThreadArgs *) v;
    volatile off_t *shared_offset = args->cp->shared_offset;
    size_t blocksize = args->cp->blocksize;
    int srcfd = args->paths->srcfd;
    int dstfd = args->paths->dstfd;

    off_t offset;
    ssize_t count;
    char buf[blocksize];

    debug(LOG_INFO, "pcp_thread Starting. Using mnt %p fd %d", args->paths,
          dstfd);
    do {
        offset = atomic_fetch_and_add_offset(shared_offset, blocksize);

        size_t iosize = blocksize;
        size_t left = iosize;

        count = 0;

        if (args->cp->length) {
            if (offset >= args->cp->length) {
                break;
            }
            if (offset + left > args->cp->length) {
                left = args->cp->length - offset;
            }
        }
        /* loop on left as read may return LESS than desired */
        while (left > 0) {
            debug(LOG_DEBUG, "pcp_thread copies offset %ld count %ld",
                  offset, left);

            count = pread(srcfd, buf, left, offset);

            if (count <= 0) {
                break;
            }

            /* if handle sparse, check of read buffer is zero */
            if (!args->cp->handle_sparse
                || memcmp(buf, __zerobuf, count) != 0) {
                /* partial writes shouldn't happen in our case */
                if (pwrite(dstfd, buf, count, offset) != count) {
                    panic(1,
                          "partial write on destination file '%s' offset %lu count %d",
                          args->paths->dstpath, offset, count);
                }
                atomic_fetch_and_add_offset(&args->cp->written_bytes,
                                            count);
            }
            left -= count;
            offset += count;
        }

    } while (count > 0);

    debug(LOG_INFO, "pcp_thread reached EOF", gettid());
    return NULL;
}

/**
 * Parse storage size strings, i.e. numbers with storage related postfixes:
 * B|b for blocks (512 bytes), k|K for KB (1024) bytes, and so one (m = MB, g = GB, t = TB, p = PB)
 */
size_t parse_storage_size(char *arg)
{
    int l = strlen(arg);
    size_t factor = 1;

    arg = strdupa(arg);
    switch (arg[l - 1]) {
    case 'P':
    case 'p':
        factor = 1llu << 50;
        break;
    case 'T':
    case 't':
        factor = 1llu << 40;
        break;
    case 'G':
    case 'g':
        factor = 1 << 30;
        break;
    case 'M':
    case 'm':
        factor = 1 << 20;
        break;
    case 'K':
    case 'k':
        factor = 1 << 10;
        break;
    case 'B':
    case 'b':
        factor = 512;
        break;
    default:
        l++;
    }
    arg[l] = 0;
    return strtoull(arg, 0, 0) * factor;
}

/**
 * Join two file system paths.
 * This should work on Windows too as it should support '/' separators.
 * @param destination
 * @param dest_size	- destination buffer size
 * @param path1		- base path
 * @param path2		- path to join
 * @return		-1 if join failed, strlen(destination) on success
 */
int path_join(char *destination, int dest_size, const char *path1,
              const char *path2)
{
    if (path1 == NULL || path2 == NULL) {
        return -1;
    }
    int p1len = strlen(path1);
    int p2len = strlen(path2);

    if (p1len > dest_size - 1) {
        return -1;
    }

    strncpy(destination, path1, dest_size);
    if (destination[p1len - 1] != '/') {
        destination[p1len] = '/';
        p1len++;
    }

    if (p1len + p2len > dest_size - 1) {
        return -1;
    }

    strncpy(destination + p1len, path2, dest_size - p1len);
    return p1len + p2len;
}

struct tm *get_timestamp(void)
{
    struct tm *buf = calloc(1, sizeof(struct tm));
    struct tm *tm;
    time_t t;

    t = time(NULL);
    tm = localtime_r(&t, buf);
    if (tm == NULL) {
        panic(1, "can't get time stamp");
    }
    return tm;
}



/**
 * Open the source and destination files according to the given parameters.
 * Mode is copied by default from the source, unless explicit mode is provide.
 * 
 * @param params	- (IN)  cp params
 * @param targs		- (OUT) threads start params
 */
void open_files(CpParams * params, PcpThreadArgs * targs)
{
    params->starttime = get_timestamp();
    /*
     * Open all src paths.
     * Check that all sources have the same inode.
     * If mode is not provided by the user, use the first path mode.
     */
    int *src_fds = params->src_fds;
    long ino = 0;
    int s;
    for (s = 0; s < params->n_srcs; s++) {
        src_fds[s] = open(params->srcpath[s], O_RDONLY);
        debug(LOG_INFO, "src path [%d] '%s' -> fd %d", s,
              params->srcpath[s], src_fds[s]);
        if (src_fds[s] < 0) {
            panic(1, "src file '%s' cannot be opened", params->srcpath[s]);
        }
        struct stat64 stat;
        if (fstat64(src_fds[s], &stat) < 0) {
            panic(1, "Can't stat source file '%s'", params->srcpath[s]);
        }
        if (params->mode == 0) {
            params->mode = stat.st_mode;
        }
        if (ino && stat.st_ino != ino) {
            panic(0,
                  "source file '%s' inode %d is not same as previous %d",
                  params->srcpath[s], stat.st_ino, ino);
        }
        ino = stat.st_ino;
    }

    /*
     * Open all dest paths.
     * Check that all destinations have the same inode.
     * First path will create and/or truncate the file, others just open.
     * 
     */
    int *dst_fds = params->dst_fds;
    int d;
    for (d = 0, ino = 0; d < params->n_dsts; d++) {
        debug(LOG_INFO, "opening destination path '%s'",
              params->dstpath[d]);
        int open_mode = O_WRONLY | (params->sync_mode ? O_SYNC : 0);
        if (d == 0 && !params->dst_stream) {
            /* when opening several paths, only the first open should create & truncate the dest file */
            open_mode |=
                (params->force) ? (O_CREAT | O_TRUNC) : (O_CREAT | O_EXCL);
        }
        dst_fds[d] = open(params->dstpath[d], open_mode, params->mode);
        debug(LOG_INFO, "dest path [%d] '%s' -> fd %d", d,
              params->dstpath[d], dst_fds[d]);
        if (dst_fds[d] < 0) {
            panic(1, "destination file '%s' cannot be opened",
                  params->dstpath[d]);
        }
        struct stat64 stat;
        if (fstat64(dst_fds[d], &stat) < 0) {
            panic(1, "Can't stat dest file '%s' after open/create",
                  params->dstpath[d]);
        }
        if (ino && stat.st_ino != ino) {
            panic(0,
                  "destination file '%s' inode %d is not same as previous %d",
                  params->dstpath[d], stat.st_ino, ino);
        }
        ino = stat.st_ino;
    }

    /*
     * Build the paths matrix. Support the option to use the first dest
     * path for create and the others for the actual IOs
     */
    int start_dst = 0;
    if (params->n_dsts > 1 && params->first_path_create_only) {
        start_dst = 1;          /* do not use first dest file */
    }

    int n_paths = 0;
    for (s = 0; s < params->n_srcs; s++) {
        for (d = start_dst; d < params->n_dsts; d++) {
            PathParams *path = params->paths + n_paths++;

            path->dstfd = dst_fds[d];
            path->srcfd = src_fds[s];
            path->dstpath = params->dstpath[d];
            path->srcpath = params->srcpath[s];
        }
    }

    /* build the thread params array - assign all paths in round robin to all threads */
    PcpThreadArgs *ta;
    int p = 0;
    for (ta = targs, p = 0; ta < targs + params->threads; ta++, p++) {
        PathParams *path = params->paths + (p % n_paths);
        debug(LOG_INFO, "Th %d will use path #%d %p (sfd %d dfd %d)",
              ta - targs, path - params->paths, path, path->srcfd,
              path->dstfd);
        ta->paths = path;
        ta->cp = params;
    }
}

void close_files(CpParams * params, PcpThreadArgs * targs)
{
    debug(LOG_INFO, "Close source");
    int s;
    for (s = 0; s < params->n_srcs; s++) {
        debug(LOG_INFO, "Close source file '%s' fd %d", params->srcpath[s],
              params->src_fds[s]);
        close(params->src_fds[s]);
    }

    debug(LOG_INFO, "Sync destination");
    int d;
    for (d = 0; d < params->n_dsts; d++) {
        debug(LOG_INFO, "Sync destination file '%s' fd %d",
              params->dstpath[d], params->dst_fds[d]);
        fsync(params->dst_fds[d]);
        close(params->dst_fds[d]);
    }
    params->endtime = get_timestamp();
    logfile(params, NULL);
}

void init_logfile(CpParams * params)
{
    char *logfile = params->logfile_path;
    int headers = 0;

    if (access(params->logfile_path, F_OK) < 0) {
        /* no such file */
        headers = 1;
    }

    params->logfile = fopen(logfile, "a");

    if (params->logfile == NULL) {
        panic(1, "can't open logfile '%s'", logfile);
    }
    if (headers) {
        /* taken from gsutil headers */
        fprintf(params->logfile,
                "Source,Destination,Start,End,Md5,UploadId,Source Size,Bytes Transferred,Result,Description\n");
    }
    debug(LOG_DEBUG, "open log file '%s'", logfile);
}

int
detach(void)
{
	int d;

	if ((d = daemon(1, 1)) > 0) {		///* detach from parent process 
		debug(LOG_ERROR, "return to parent, child is %d", d);
		return 0;
	}
	if (d < 0) {
		panic(1, "daemon failed %d", d);
		return -1;
	}
	return 0;
}

typedef struct sys_md {
	pthread_mutex_t mutex;
	int initialized;
	pthread_cond_t cond;
	int volatile free_threads;
} SysMD;

SysMD *
attach_sys_md(char *file, int sys_max_threads)
{
	SysMD *sysmd;
	int creator = 0;
	int fd = -1;

	for (;;) {
		/* check if I am the creator of the md file */
		if (access(file, F_OK) == -1) {
			debug(LOG_DEBUG, "No sys md file '%s' - try to create it", file);
			creator = 1;
		}
		fd = open(file, O_RDWR| (creator ? O_EXCL | O_CREAT : 0), 0700);
		if (fd < 0) {
			if (errno == EEXIST) {	/* tried to create it exclusively but failed */
				debug(LOG_DEBUG, "Lost in race to create '%s' retry.", file);
				continue;	/* I am racing with someone and I lost. Try again. */
			}
			panic (1, "open shm file '%s' failed ", file);
		}
		/* managed to open the file - continue with the flow */
		debug(LOG_DEBUG, "Md file '%s' - opened", file);
		break;
	}

	if (creator) {
		SysMD initmd;
		/* init the structure */
		debug(LOG_DEBUG, "Initialize '%s' to %d max threads", file, sys_max_threads);

		/* Initialise attribute to shared mutex. */
		pthread_mutexattr_t attrmutex;
		pthread_mutexattr_init(&attrmutex);
		pthread_mutexattr_setpshared(&attrmutex, PTHREAD_PROCESS_SHARED);
		pthread_mutex_init(&initmd.mutex, &attrmutex);

		/* Initialise attribute to shared condition. */
		pthread_condattr_t attrcond;
		pthread_condattr_init(&attrcond);
		pthread_condattr_setpshared(&attrcond, PTHREAD_PROCESS_SHARED);
		pthread_cond_init(&initmd.cond, &attrcond);
		initmd.free_threads = sys_max_threads;
		initmd.initialized = 1;
		if (write(fd, &initmd, sizeof initmd) < 0) {
			panic(1, "create sysmd file '%s' failed ", file);
		}
		//msync(sysmd, sizeof initmd, MS_SYNC);	
	} else {
		/* ensure that the struct is initialized, if not try to wait */
		for(;;) {
			struct stat stat;
			if (fstat(fd, &stat) < 0) {
				panic(1, "SysMD file '%s' is gone?", file);
			}
			
			if (stat.st_size < sizeof(SysMD)) {
				usleep(10000);
				continue;
			}
			break;
		}
	}

	void *p = mmap(NULL, sizeof (*sysmd), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
	if (p == MAP_FAILED) {
		panic(1, "attach sys md '%s' failed ", file);
	}

	sysmd = (SysMD *)p;
	if (!sysmd->initialized) {
		panic(0, "timed out waiting for sys md '%s' to initialize. Is it corrupted?", file);
	}
	
	return sysmd;
}

void
wait_sys_threads(SysMD *sysmd, int threads) 
{
	debug(LOG_DEBUG, "SysMD: Allocate %d threads", threads);
	pthread_mutex_lock(&sysmd->mutex);
	while (sysmd->free_threads < threads) {
		debug(LOG_DEBUG, "SysMD: Wait for %d threads, available now %d", threads, sysmd->free_threads);
		pthread_cond_wait(&sysmd->cond, &sysmd->mutex);
	}
	sysmd->free_threads -= threads;
	int left = sysmd->free_threads;
	pthread_mutex_unlock(&sysmd->mutex);
	debug(LOG_DEBUG, "SysMD: Done allocating %d threads, left %d", threads, left);
}

void
free_sys_threads(SysMD *sysmd, int threads) 
{
	debug(LOG_DEBUG, "SysMD: Free %d threads", threads);
	pthread_mutex_lock(&sysmd->mutex);
	sysmd->free_threads += threads;
	int total = sysmd->free_threads;
	pthread_mutex_unlock(&sysmd->mutex);
	pthread_cond_broadcast(&sysmd->cond);
	debug(LOG_DEBUG, "SysMD: total %d threads are free", total);
}

int main(int argc, char **argv)
{
    pthread_t thread_ids[THREADS_NUM_MAX], stream_thread;
    PcpThreadArgs targs[THREADS_NUM_MAX];
    volatile off_t shared_offset;
    void *(*thread_fn) (void *) = pcp_thread;
	TarFile tarfile;
	SysMD *sysmd = NULL;
	int peek_md = 0;

    CpParams params = {
        .srcpath = "",
        .dstpath = "",
        .shared_offset = &shared_offset,
        .handle_sparse = 0,
        .paths = {0},
        .n_srcs = 0,
        .n_dsts = 0,
        .src_stream = 0,
        .dst_stream = 0,
        .force = 0,
        .mode = 0,
        .threads = THREADS_NUM_DEF,
        .blocksize = BLOCK_SIZE_DEF,
        .logfile_path = NULL,
		.sys_max_threads = SYS_THREADS_NUM_DEF,
    };
    int i;

    __progname = argv[0];
    error_params = &params;     /* for panics */

    /* check for multipath mode, i.e. if we have a "--" param */
    int multipath_mode = 0;

    for (i = 1; i < argc; i++) {
        char *opt = argv[i];
        if (strcmp(opt, "--") == 0) {
            multipath_mode = 1;
            debug(LOG_INFO, "Multipath mode is used.");
        }
    }

    for (i = 1; i < argc; i++) {
        char *opt = argv[i];

        if (opt[0] != '-' || strlen(opt) < 2) {
            break;              /* POSIX - flags before arguments */
        }
        switch (opt[1]) {
		case 'a':
            params.async = 1;
			break;
        case 'f':
            params.force = 1;
            break;
        case 'd':
            __log_level++;
            break;
        case 'h':
            usage();
            break;
        case 'C':
            params.first_path_create_only = 1;
            break;
        case 'b':
            if (argc < i + 1) {
                panic(0, "parameter for flag '-b <blocksize>' is missing");
            }
            params.blocksize = parse_storage_size(argv[i + 1]);
            if (params.blocksize < BLOCK_SIZE_MIN
                || params.blocksize > BLOCK_SIZE_MAX) {
                panic(0,
                      "Bad block size: %d is missing - allowed range: [%d-%d]",
                      params.blocksize, (int) BLOCK_SIZE_MIN,
                      (int) BLOCK_SIZE_MAX);
            }
            i++;
            break;
        case 'l':
            if (argc < i + 1) {
                panic(0, "parameter for flag '-l <length>' is missing");
            }
            params.length = parse_storage_size(argv[i + 1]);
            i++;
            break;
        case 'L':
            if (argc < i + 1) {
                panic(0, "parameter for flag '-L <logfile>' is missing");
            }
            params.logfile_path = argv[i + 1];
            init_logfile(&params);
            i++;
            break;
        case 'm':
            if (argc < i + 1) {
                panic(0,
                      "parameter for mode '-m <octal mode>' is missing");
            }
            params.mode = strtoul(argv[i + 1], NULL, 8);
            if (params.mode == 0) {
                panic(0,
                      "Bad mode parameter for '-m <mode>' - use octal notation, e.g. 755");
            }
            i++;
            break;
        case 'S':
            params.handle_sparse = 1;
            break;
        case 's':
            params.sync_mode = 1;
            break;
        case 'T':
            params.tarmode = 1;
            break;
        case 't':
            if (argc < i + 1) {
                panic(0,
                      "parameter for threads '-t <threads_num>' is missing");
            }
            params.threads = strtoul(argv[i + 1], NULL, 0);
            if (params.threads <= 0 || params.threads > THREADS_NUM_MAX) {
                panic(0, "Bad mode parameter for '-t <threads_num>' - range [1 - %d] is supported",
                      THREADS_NUM_MAX);
            }
            i++;
            break;
		case 'M':
			if (argc < i + 1) {
                panic(0, "parameter for flag '-M <sys_md file>' is missing");
            }
			params.sys_md_file = argv[i + 1];
            i++;
			break;
		case 'P':
			if (argc < i + 1) {
                panic(0, "parameter for flag '-M <sys_md file>' is missing");
            }
			params.sys_md_file = argv[i + 1];
			peek_md = 1;
            i++;
			break;
		case 'X':
            if (argc < i + 1) {
                panic(0, "parameter for threads '-X <threads_num>' is missing");
            }
            params.sys_max_threads = strtoul(argv[i + 1], NULL, 0);
            if (params.sys_max_threads <= 0 || params.sys_max_threads > SYS_THREADS_NUM_MAX) {
                panic(0, "Bad mode parameter for '-X <threads_num>' - range [1 - %d] is supported",
                      SYS_THREADS_NUM_MAX);
            }
            i++;
            break;
        case 'v':
            printf("%s version %s\n", __progname, commit);
            exit(0);
            break;
        case '-':
            panic(0,
                  "multi source or destination mode is set ('--' flag) used but no source is found before the '--' argument");
            break;
        default:
            panic(0, "unknown flag '%c'", opt[1]);
            break;
        }
    }

    argc -= i - 1;
    argv += i - 1;

	if (peek_md) {
		sysmd = attach_sys_md(params.sys_md_file, params.sys_max_threads);
		printf("%s: free threads %d\n", params.sys_md_file, sysmd->free_threads);
		exit(0);
	}

	/* tar mode: file params are passed via env params. See man tar, --to-command section */
    if (params.tarmode || !strcmp(basename(__progname), "tarpcp")) {
        params.tarmode = 1;
        params.force = 1;
		params.async = 1;
		/*if (params.sys_md_file == NULL) { 
			params.sys_md_file = malloc(256);
			snprintf(params.sys_md_file, 255,"/tmp/tarsync.%s", getenv("USER"));
		}*/
        //__log_level = LOG_DEBUG;
        tar_cp(&tarfile);
        params.n_dsts = 1;
        params.n_srcs = 1;
        params.srcpath[0] = "-";	/* stdin */
        params.dstpath[0] = tarfile.filename;
		if (tarfile.filename == NULL) {
			panic(0, "no target file is specified, aborting.");
		}
		int threads = tarfile.stat.st_size / params.blocksize;
		if (threads > params.threads) {
			threads = params.threads;
		}
		if (threads < 1) {
			threads = 1;
		}
		debug(LOG_DEBUG, "Tar mode: using %d threads", threads);
		params.threads = threads;
		params.tarparams = &tarfile;
        params.mode = tarfile.stat.st_mode;
    } else {
		/* standard mode, get args from command line */
		if ((!multipath_mode && argc < 3) || (multipath_mode && argc < 4)) {
			usage();
		}
	
		if (!multipath_mode) {
			params.n_dsts = 1;
			params.n_srcs = 1;
			params.srcpath[0] = argv[1];
			params.dstpath[0] = argv[2];
		} else {
			int src = 1;

			for (i = 1; i < argc; i++) {
				char *opt = argv[i];
				if (strcmp(opt, "--") == 0) {
					src = 0;
					continue;
				}
				if (src) {
					if (params.n_srcs >= MNT_POINTS_MAX) {
						panic(0, "Source files number exceeds max %d",
							MNT_POINTS_MAX);
					}
					params.srcpath[params.n_srcs] = opt;
					debug(LOG_DEBUG, "Add src %s", opt);
					params.n_srcs++;
				} else {
					if (params.n_dsts >= MNT_POINTS_MAX) {
						panic(0, "Target files number exceeds max %d",
							MNT_POINTS_MAX);
					}
					params.dstpath[params.n_dsts] = opt;
					debug(LOG_DEBUG, "Add dest %s", opt);
					params.n_dsts++;
				}
			}
			if (params.n_dsts == 0 || params.n_dsts == 0) {
				panic(0,
					"At least one destination and one source must be specified in multipath mode");
			}
		}
	}

    /* check for streams in or out - specified by filename == "-" */
    for (i = 0; i < params.n_srcs; i++) {
        if (strcmp(params.srcpath[i], "-") == 0) {
            params.src_stream = 1;
            params.srcpath[i] = "/dev/stdin";
        }
    }
    for (i = 0; i < params.n_dsts; i++) {
        if (strcmp(params.dstpath[i], "-") == 0) {
            params.dst_stream = 1;
            params.dstpath[i] = "/dev/stdout";
        }
    }
    if (params.src_stream && params.n_srcs > 1) {
        panic(0, "Source is stream but multiple sources are defined");
    }
    if (params.dst_stream && params.n_dsts > 1) {
        panic(0,
              "Destination is stream but multiple destinations are defined");
    }

	if (params.async) {
        debug(LOG_INFO, "Async mode: before fork");
		/* child */
		detach();
        debug(LOG_INFO, "Async mode: background child is starting");
	}

	if (params.sys_md_file) {
		sysmd = attach_sys_md(params.sys_md_file, params.sys_max_threads);
		wait_sys_threads(sysmd, params.threads);
	}

    open_files(&params, targs);

    if (params.src_stream) {
        /* init two io buffers per thread, put all buffer in free q, init empty IO q */
        qinit(&params.stream_free, params.threads * 2, params.blocksize);
        qinit(&params.stream_io, 0, 0);
        debug(LOG_DEBUG, "Starting reading from stdin...");
        if (pthread_create
            (&stream_thread, NULL, pcp_stream_reader, &targs[0])) {
            panic(1, "Thread create failed for steram read thread");
        }
        thread_fn = pcp_cp_from_stream;
    }

    if (params.dst_stream) {
        /* init two io buffers per thread, put all buffer in free q, init empty IO q */
        qinit(&params.stream_free, params.threads * 2, params.blocksize);
        qinit(&params.stream_io, 0, 0);
        debug(LOG_DEBUG, "Starting writing to stdout...");
        if (pthread_create
            (&stream_thread, NULL, pcp_stream_writer, &targs[0])) {
            panic(1, "Thread create failed for steram read thread");
        }
        thread_fn = pcp_cp_to_stream;
    }

    debug(LOG_DEBUG, "Starting copy...");
    for (i = 0; i < params.threads; i++) {
        if (pthread_create(&thread_ids[i], NULL, thread_fn, &targs[i])) {
            panic(1, "Thread create failed for thread #%d", i);
        }
    }
    for (i = 0; i < params.threads; i++) {
        if (pthread_join(thread_ids[i], NULL)) {
            panic(1, "Thread wait failed on thread #%d", &targs[i]);
        }
    }

    if (params.dst_stream) {
        qexit(&params.stream_io);       /* signal exit on IO q to free stream writer thread */
        debug(LOG_INFO, "Wait for stream writer");
        if (pthread_join(stream_thread, NULL)) {
            panic(1, "Thread wait failed on stream writer %lu",
                  (ulong) stream_thread);
        }
    }

    close_files(&params, targs);

	if (params.sys_md_file) {
		free_sys_threads(sysmd, params.threads);
	}

    debug(LOG_INFO, "Done");
}
