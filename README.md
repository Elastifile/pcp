# PCP - Parallel Copy Utility for Linux

Copy a single file using parallel IO optimally using multiple sources and/or multiple targets.

# Main features
* Multithreaded read and write
* User control over block size
* User control over the amount of data copies
* Sparse file support
* Sync IO support (O_SYNC). The default is the standard async IO.
* Support for multiple mounts (file views) of the same file. This is useful when reading or writing to a multi-head file server
* Support from reading from/to a pipe
* TAR filter support - get the source/target file details via ENV (see tar)
* Background mode - move the process to background before doing IO (but after creating it)
* gsutil style loggin

Note that pcp always copies a **single file**, all sources and/or targets are assumed to be the same file (but using different mounts, etc.).

## Build

    make
 
### Additional make targets
 * **test**: show files, libraries, headers, etc.
 * **pcp32**: build 32 bit version. The default for 64 systems is 64 bit build.
 * **clean**: remove all build outputs 

## Usage

    Usage: ./pcp [-b <blocksize> -m <octal mode> -f -d -s] <src file> <dest file>
    Usage: ./pcp [-b <blocksize> -m <octal mode> -f -d -s] <src file1> <src file2> < src file-n> -- <dest file1> <dest file2> <dest file n>
    Usage: ./pcp -T 

	-h			Print the usage screen (this screen).
	-b <blocksize>		IO blocksize to use [ 1 - 1048576 ], prefixes bKMGT are supported. Default 65536.
	-l <length>		Max bytes to read from source. prefixes bKMGT are supported. Default 0 = all file.
	-d			Increase debug level. Default 1.
	-f			Force - allow destination file overwrite. Default - fail if destination file exists.
	-m <octal mode>		destination file mode if created. Default <source> mode.
	-t <threads_num>	Number of threads to use [1-256]. Default 16.
	-S			Sparse file support. Default NO.
	-s			Sync IO mode. Default async.
	-v			Print version and exit.
	-C			Create/Open the file on first mount, but do the IOs on other mounts. Relevant only if -M is used. Default NO.
	-L <logfile>		Log the operation in the specified file, mimicking gsutil cp -L.
	-T			Tar mode. No args are provides, real args are read from env (see tar --to-command section)
	-X <threads>		Max system threads. This is set by the first process to create the sys md file (see -M). Default: 64
	-M <mdfile> 		System MD file. This file is used to coordinate the concurrent running threads.
				The first process creates it and the other attach them self to it.
	-P <mdfile> 		Peek into a system MD file - show the free threads and exit.
	-a			Async (background) mode. The process moves itself to background before doing IO.

## License
MIT
