/* for strdupa */
#define _GNU_SOURCE

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <syscall.h>
#include <time.h>
#include <errno.h>
#include <limits.h>
#include <sys/sysmacros.h>
#include <ctype.h>
#include "pcp.h"

#define bool    int
#define BILLION  1000000000
#define LOG10_BILLION 9

/* True if the real type T is signed.  */
#define TYPE_SIGNED(t) (! ((t) 0 < (t) -1))
#define TYPE_WIDTH(t) (sizeof (t) * CHAR_BIT)

/* The maximum and minimum values for the integer type T.  */
#define TYPE_MAXIMUM(t)                                                 \
  ((t) (! TYPE_SIGNED (t)                                               \
        ? (t) -1                                                        \
        : ((((t) 1 << (TYPE_WIDTH (t) - 2)) - 1) * 2 + 1)))

#define TYPE_MINIMUM(t) ((t) ~ TYPE_MAXIMUM (t))

enum { TIMESPEC_STRSIZE_BOUND = 64 };

char const *code_timespec(struct timespec t,
                          char sbuf[TIMESPEC_STRSIZE_BOUND])
{
    time_t s = t.tv_sec;
    int ns = t.tv_nsec;
    bool negative = s < 0;

    /* ignore invalid values of ns */
    if (BILLION <= ns || ns < 0)
        ns = 0;

    if (negative && ns != 0) {
        s++;
        ns = BILLION - ns;
    }

    snprintf(sbuf, TIMESPEC_STRSIZE_BOUND - 1, "%ld.%d", (long) s, ns);
    return sbuf;
}

struct timespec
decode_timespec(char const *arg, char **arg_lim, bool parse_fraction)
{
    time_t s = TYPE_MINIMUM(time_t);
    int ns = -1;
    char const *p = arg;
    bool negative = *arg == '-';
    struct timespec r;

    if (!isdigit(arg[negative]))
        errno = EINVAL;
    else {
        errno = 0;

        if (negative) {
            long i = strtol(arg, arg_lim, 10);
            if (TYPE_SIGNED(time_t) ? TYPE_MINIMUM(time_t) <= i : 0 <= i)
                s = i;
            else
                errno = ERANGE;
        } else {
            ulong i = strtoul(arg, arg_lim, 10);
            if (i <= TYPE_MAXIMUM(time_t))
                s = i;
            else
                errno = ERANGE;
        }

        p = *arg_lim;
        ns = 0;

        if (parse_fraction && *p == '.') {
            int digits = 0;
            bool trailing_nonzero = 0;

            while (isdigit(*++p))
                if (digits < LOG10_BILLION)
                    digits++, ns = 10 * ns + (*p - '0');
                else
                    trailing_nonzero |= *p != '0';

            while (digits < LOG10_BILLION)
                digits++, ns *= 10;

            if (negative) {
                /* Convert "-1.10000000000001" to s == -2, ns == 89999999.
                   I.e., truncate time stamps towards minus infinity while
                   converting them to internal form.  */
                ns += trailing_nonzero;
                if (ns != 0) {
                    if (s == TYPE_MINIMUM(time_t))
                        ns = -1;
                    else {
                        s--;
                        ns = BILLION - ns;
                    }
                }
            }
        }

        if (errno == ERANGE)
            ns = -1;
    }

    *arg_lim = (char *) p;
    r.tv_sec = s;
    r.tv_nsec = ns;
    return r;
}

static char *env_to_str(char *key)
{
    return getenv(key);
}

static long long env_to_int(char *key)
{
    char *s = getenv(key);
    if (s == NULL) {
        return 0;
    }
    return strtoll(s, 0, 0);
}

static char env_to_char(char *key)
{
    return env_to_int(key);
}

static struct timespec env_to_timespec(char *key)
{
    char *s = env_to_str(key);
    if (s == NULL) {
        struct timespec tmp = { 0, 0 };
        return tmp;
    }
    char *p = strchr(s, '.');
    if (p == NULL) {
        p = s + strlen(s);
    }
    return decode_timespec(s, &p, 1);
}

static void env_to_stat(TarFile * tar_file)
{
    tar_file->filename = env_to_str("TAR_FILENAME");
    tar_file->realname = env_to_str("TAR_REALNAME");
    tar_file->linkname = env_to_str("TAR_LINKNAME");

    tar_file->version = env_to_str("TAR_VERSION");
    tar_file->archive = env_to_str("TAR_ARCHIVE");
    tar_file->volume = env_to_int("TAR_VOLUME");
    tar_file->blocking_factor = env_to_int("TAR_BLOCKING_FACTOR");
    tar_file->format = env_to_str("TAR_FORMAT");
    tar_file->filetype = env_to_char("TAR_FILETYPE");
    tar_file->stat.st_mode = env_to_int("TAR_MODE");
    tar_file->uname = env_to_str("TAR_UNAME");
    tar_file->gname = env_to_str("TAR_GNAME");
    tar_file->atime = env_to_timespec("TAR_ATIME");
    tar_file->mtime = env_to_timespec("TAR_MTIME");
    tar_file->ctime = env_to_timespec("TAR_CTIME");
    tar_file->stat.st_size = env_to_int("TAR_SIZE");
    tar_file->stat.st_uid = env_to_int("TAR_UID");
    tar_file->stat.st_gid = env_to_int("TAR_GID");
    tar_file->stat.st_rdev =
        makedev(env_to_int("TAR_MAJOR"), env_to_int("TAR_MINOR"));
}

static void debug_env(TarFile * tar_file)
{
    debug(LOG_DEBUG, "TAR_FILENAME: %s", tar_file->filename);
    debug(LOG_DEBUG, "TAR_REALNAME: %s", tar_file->realname);
    debug(LOG_DEBUG, "TAR_LINKNAME: %s", tar_file->linkname);

    debug(LOG_DEBUG, "TAR_VERSION: %s", tar_file->version);
    debug(LOG_DEBUG, "TAR_ARCHIVE: %s", tar_file->archive);
    debug(LOG_DEBUG, "TAR_VOLUME: %d", tar_file->volume);
    debug(LOG_DEBUG, "TAR_BLOCKING_FACTOR: %d",
          tar_file->blocking_factor);
    debug(LOG_DEBUG, "TAR_FORMAT: %s", tar_file->format);
    debug(LOG_DEBUG, "TAR_FILETYPE: %d", (int) tar_file->filetype);
    debug(LOG_DEBUG, "TAR_MODE: 0%lo", (ulong) tar_file->stat.st_mode);
    debug(LOG_DEBUG, "TAR_UNAME: %s", tar_file->uname);
    debug(LOG_DEBUG, "TAR_GNAME: %s", tar_file->gname);
    debug(LOG_DEBUG, "TAR_ATIME: %s (%s)",
          code_timespec(tar_file->atime, alloca(64)), getenv("TAR_ATIME"));
    debug(LOG_DEBUG, "TAR_MTIME: %s (%s)",
          code_timespec(tar_file->mtime, alloca(64)), getenv("TAR_MTIME"));
    debug(LOG_DEBUG, "TAR_CTIME: %s (%s)",
          code_timespec(tar_file->ctime, alloca(64)), getenv("TAR_CTIME"));
    debug(LOG_DEBUG, "TAR_SIZE: %llu",
          (unsigned long long) tar_file->stat.st_size);
    debug(LOG_DEBUG, "TAR_UID: %d", (int) tar_file->stat.st_uid);
    debug(LOG_DEBUG, "TAR_GID: %d", (int) tar_file->stat.st_gid);
    debug(LOG_DEBUG, "st_rdev: %lu (%s %s)",
          (ulong) tar_file->stat.st_rdev, getenv("TAR_MAJOR"),
          getenv("TAR_MINOR"));

}

void tar_cp(TarFile *tarfile)
{
    env_to_stat(tarfile);
    debug_env(tarfile);
}
