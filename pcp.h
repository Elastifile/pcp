#ifndef __PCP_H
#define __PCP_H

#include <time.h>
#include <sys/stat.h>

enum LogLevels {
    LOG_NONE = 0,
    LOG_ERROR,
    LOG_INFO,
    LOG_DEBUG,
    LOG_VERBOSE,
};

void panic(int err, char *msg, ...);
void debug(int level, char *msg, ...);

typedef struct TarFile {
    char *filename;
    char *realname;
    char *linkname;
    char *version;
    char *archive;
    int volume;
    int blocking_factor;
    char *format;
    char filetype;
    char *uname;
    char *gname;
    struct timespec atime;
    struct timespec mtime;
    struct timespec ctime;
    struct stat stat;
} TarFile;

void tar_cp(TarFile *tarfile);

#endif
