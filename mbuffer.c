#include "config.h"
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/timeb.h>
#include <sys/mman.h>
#include <unistd.h>


pthread_t Reader, Writer;
int Verbose = 3, Finish = 0, In, Out, Tmp, Rest = 0, Numin = 0,
    Numout = 0, Pause = 0, Memmap = 0;
float Start = 0;
char *Tmpfile = 0;
char **Buffer;
int Blocksize = 10240;
int Numblocks = 256;
sem_t Dev2Buf,Buf2Dev;
FILE* Log;


void debugmsg(const char *msg, ...)
{
#ifdef DEBUG
	va_list val;
	if (Verbose < 5)
		return;
	va_start(val,msg);
	vfprintf(Log,msg,val);
	va_end(val);
#endif
}

void infomsg(const char *msg, ...)
{
	va_list val;
	if (Verbose < 4)
		return;
	va_start(val,msg);
	vfprintf(Log,msg,val);
	va_end(val);
}

void errormsg(const char *msg, ...)
{
	va_list val;

	if (Verbose < 2)
		return;
	va_start(val,msg);
	vfprintf(Log,msg,val);
	va_end(val);
	exit(-1);
}


void fatal(const char *msg, ...)
{
	va_list val;

	if (Verbose < 1)
		return;
	va_start(val,msg);
	vfprintf(Log,msg,val);
	va_end(val);
	exit(-1);
}

void terminate()
{
	infomsg("\rterminating...\n");
	pthread_cancel(Reader);
	pthread_cancel(Writer);
	remove(Tmpfile);
	exit(0);
}

RETSIGTYPE sigHandler(int signr)
{
	switch (signr) {
	case SIGINT:
		infomsg("\rcatched INT signal...\n");
		terminate();
	case SIGTERM:
		infomsg("\rcatched TERM signal...\n");
		terminate();
	}
}

void statusThread() 
{
	int rest, total;
	struct timeb last, now;
	float in = 0, out = 0, diff, fill;
	int lin = 0, lout = 0;

	ftime(&last);
	usleep(1000);	// needed on alpha (stderr fails with fpe on nan)
	sem_getvalue(&Buf2Dev,&rest);
	while (!(Finish & (rest == 0))) {
		fill = (float)rest / (float)Numblocks * 100;
		ftime(&now);
		diff = now.time - last.time + (float) now.millitm / 1000 - (float) last.millitm / 1000;
		in = ((Numin - lin) * Blocksize) >> 10;
		in /= diff;
		out = ((Numout - lout) * Blocksize) >> 10;
		out /= diff;
		lin = Numin;
		lout = Numout;
		last.time = now.time;
		last.millitm = now.millitm;
		total = (Numout*Blocksize)>>10;
		fprintf(Log,"\rin at %8.1f kB/sec - out at %8.1f kB/sec - %i kB totally transfered - buffer %3.0f%% full",in,out,total,fill);
		fflush(Log);
		usleep(500000);
		sem_getvalue(&Buf2Dev,&rest);
	}
	infomsg("statusThread: joining to terminate...\n");
	pthread_join(Reader,0);
	pthread_join(Writer,0);
	if (Memmap)
		munmap(Buffer[0],Blocksize*Numblocks);
	close(Tmp);
	remove(Tmpfile);
	exit(0);
}

void inputThread()
{
	int at = 0, err, num;

	infomsg("inputThread: starting...\n");
	while (!Finish) {
		debugmsg("inputThread: wait\n");
		sem_wait(&Dev2Buf);
		num = 0;
		do {
			debugmsg("inputThread: read\n");
			err = read(In,Buffer[at] + num,Blocksize - num);
			if (-1 == err) {
				errormsg("error reading: %s\n",strerror(errno));
				pthread_exit((void *) 0);
			} else if (0 == err) {
				Finish = 1;
				Rest = num;
				debugmsg("inputThread: last block has %i bytes\n",Rest);
				sem_post(&Buf2Dev);
				infomsg("inputThread: exiting...\n");
				pthread_exit(0);
			}
			num += err;
		} while (num < Blocksize);
		debugmsg("inputThread: post\n");
		sem_post(&Buf2Dev);
		at++;
		if (at == Numblocks)
			at = 0;
		Numin++;
	}
	infomsg("inputThread: exiting...");
}

void outputThread()
{
	int at = 0, err, fill = 0, num;
	
	if (Start)
		debugmsg("outputThread: waiting for buffer...\n");
	while (((float) fill / (float) Numblocks) < Start) {
		usleep(100000);
		sem_getvalue(&Buf2Dev,&fill);
	}
	infomsg("outputThread: starting...\n");
	while (1) {
		debugmsg("outputThread: wait\n");
		sem_wait(&Buf2Dev);
		num = 0;
		if (Finish) {
			sem_getvalue(&Buf2Dev,&fill);
			if ((0 == Rest) && (0 == fill)) {
				infomsg("outputThread: exiting...\n");
				pthread_exit(0);
			}
			if (0 == fill) {
				debugmsg("outputThread: last block has %i bytes\n",Rest);
				Blocksize = Rest;
			}
		}
		do {
			debugmsg("outputThread: write %i\n",-num);
			err = write(Out,Buffer[at++] + num,Blocksize - num);
			usleep(Pause);
			if (-1 == err) {
				errormsg("\nerror writing: %s\n",strerror(errno));
				Finish = 1;
				pthread_exit((void *) -1);
			}
			num += err;
		} while (num < Blocksize);
		debugmsg("outputThread: post\n");
		if (Finish && (0 == fill)) {
			infomsg("outputThread: exiting...\n");
			close(Out);
			pthread_exit(0);
		}
		sem_post(&Dev2Buf);
		if (Numblocks == at)
			at = 0;
		Numout++;
	}
}

int getNumber(const char *str)
{
	int i;
	char c;

	switch (sscanf(str,"%i%c",&i,&c)) {
	case 0:
		return -1;	// error
	case 1:
		return i;
	default:
	}
	switch (c) {
	case 'k':
	case 'K':
		return i << 10;
	case 'M':
		return i << 20;
	}
	return -1;
}

void version()
{
	fprintf(stderr,
		"mbuffer version "VERSION"\n"\
		"Copyright 2001 - T. Maier-Komor\n"
		"License: GPL2 - see file COPYING\n");
	exit(0);
}

void usage()
{
	fprintf(stderr,
		"mbuffer [Options]\n\n"\
		"Options:\n"\
		"-b <num>   : use <num> blocks for buffer (default %i)\n"\
		"-s <size>  : use block of <size> bytes for buffer (default %i)\n"\
		"-m <size>  : use buffer of a total of <size> bytes\n"
#ifdef HAVE_MMAP
		"-h         : use memory mapped i/o for huge buffer\n"
#endif
		"-p <num>   : start writing after buffer has been filled <num>%%\n"\
		"-i <file>  : use <file> for input\n"\
		"-o <file>  : use <file> for output\n"\
		"-t <file>  : use <file> as buffer (implies -m)\n"\
		"-l <file>  : use <file> for logging messages\n"\
		"-u <num>   : pause <num> microseconds after each write\n"\
		"-f         : overwrite existing files\n"\
		"-v <level> : set verbose level to <level> (valid values are 0..5)\n"\
		"-q         : quiet (equivalent to -v 0)\n"\
		"--version  : print version information\n\n"\
		"Unsupported buffer options: -t -Z -B\n",
		Numblocks,Blocksize);
	exit(0);
}

int calcint(char **argv, int c, int d)
{
	char ch;
	int i;
	
	switch (sscanf(argv[c],"%i%c",&i,&ch)) {
	case 2:
		switch (ch) {
		case 'k':
			i <<= 10;
			return i;
		case 'M':
			i <<= 20;
		case 'b':
		case 'B':
			return i;
		default:
			errormsg("unrecognized size charakter '%c' in option\n",c);
			return d;
		}
	case 1:
		return i;
	}
	errormsg("unrecognized argument \"%s\" for option \"%s\"\n",argv[c],argv[c-1]);
	return d;
}

int argcheck(const char *opt, char **argv, int *c)
{
	if (strncmp(opt,argv[*c],2))
		return 1;
	if (strlen(argv[*c]) > 2)
		argv[*c] += 2;
	else
		(*c)++;
	return 0;
}

int main(int argc, char **argv)
{
	int c, nooverwrite = O_EXCL, totalmem = 0;
	const char *inFile = 0, *outFile = 0;
	int optMset = 0, optSset = 0, optBset = 0;
	
	Log = stderr;
	for (c = 1; c < argc; c++) {
		if (!argcheck("-s",argv,&c)) {
			Blocksize = calcint(argv,c,Blocksize);
			optSset = 1;
			debugmsg("Blocksize set to %i\n",Blocksize);
		} else if (!argcheck("-m",argv,&c)) {
			totalmem = calcint(argv,c,totalmem);
			optMset = 1;
			debugmsg("totalmem set to %i\n",totalmem);
		} else if (!argcheck("-b",argv,&c)) {
			Numblocks = (atoi(argv[c])) ? (atoi(argv[c])) : Numblocks;
			optBset = 1;
			debugmsg("Numblocks set to %i\n",Numblocks);
		} else if (!argcheck("-v",argv,&c)) {
			Verbose = (atoi(argv[c])) ? (atoi(argv[c])) : Verbose;
			debugmsg("Verbose set to %i\n",Verbose);
		} else if (!argcheck("-u",argv,&c)) {
			Pause = (atoi(argv[c])) ? (atoi(argv[c])) : Pause;
			debugmsg("Pause set to %i\n",Pause);
		} else if (!argcheck("-i",argv,&c)) {
			inFile = argv[c];
			debugmsg("inFile set to %s\n",inFile);
		} else if (!argcheck("-o",argv,&c)) {
			outFile = argv[c];
			debugmsg("outFile set to %s\n",outFile);
		} else if (!argcheck("-t",argv,&c)) {
			Tmpfile = argv[c];
			Memmap = 1;
			debugmsg("Tmpfile set to %s\n",Tmpfile);
		} else if (!argcheck("-l",argv,&c)) {
			Log = fopen(argv[c],"w");
			if (0 == Log) {
				Log = stderr;
				errormsg("error opening log file: %s\n",strerror(errno));
			}
			debugmsg("logFile set to %s\n",argv[c]);
#ifdef HAVE_MMAP
		} else if (!strcmp("-h",argv[c])) {
			Memmap = 1;
			debugmsg("mm set to 1\n");
#endif
		} else if (!strcmp("-f",argv[c])) {
			nooverwrite = 0;
			debugmsg("overwrite set to 0\n");
		} else if (!strcmp("-q",argv[c])) {
			fprintf(stderr,"setting Verbose to 0\n");
			Verbose = 0;
		} else if (!argcheck("-p",argv,&c)) {
			if (1 != sscanf(argv[c],"%f",&Start))
				Start = 0;
			debugmsg("Start set to %f%%\n",Start);
		} else if (!strcmp("--help",argv[c])) {
			usage();
		} else if (!strcmp("-h",argv[c])) {
			usage();
		} else if (!strcmp("--version",argv[c])) {
			version();
		} else
			fatal("unknown argument \"%s\"\n",argv[c]);
	}

	/* consistency check for options */
	if (optBset&optSset&optMset) {
		if (Numblocks * Blocksize != totalmem)
			fatal("inconsistent options: blocksize * number of blocks != totalsize!\n");
	} else if ((!optBset&optSset&optMset) || (optMset&!optBset&!optSset)) {
		Numblocks = totalmem / Blocksize;
		infomsg("Numblocks set to %i\n",Numblocks);
	} else if (optBset&!optSset&optMset) {
		Blocksize = totalmem / Numblocks;
		infomsg("blocksize set to %i\n",Blocksize);
	}

	/* create buffer */
	Buffer = (char **) malloc(Numblocks * sizeof(char *));
	if (!Buffer)
		fatal("Could not allocate enough memory!\n");
#ifdef HAVE_MMAP
	if (Memmap) {
		infomsg("mapping temporary file to memory with %i blocks with %i byte (%i kB total)...\n",Numblocks,Blocksize,Numblocks*Blocksize/1024);
		if (!Tmpfile) {
			Tmpfile = malloc(20*sizeof(char));
			if (!Tmpfile)
				fatal("out of memory.\n");
			strcpy(Tmpfile,"/tmp/mbuffer-XXXXXX");
		}
		Tmp = mkstemp(Tmpfile);
		if (-1 == Tmp)
			fatal("could not create temporary file (%s): %s\n",tmpfile,strerror(errno));
		/* resize the file. Needed - at least under linux, who knows why? */
		lseek(Tmp,Numblocks*Blocksize-sizeof(int),SEEK_SET);
		write(Tmp,&c,sizeof(int));
		Buffer[0] = mmap(0,Blocksize*Numblocks,PROT_READ|PROT_WRITE,MAP_PRIVATE,Tmp,0);
		if (MAP_FAILED == Buffer[0])
			perror("could not map buffer-file to memory");
		debugmsg("temporary file mapped to address %p\n",Buffer[0]);
	} else {
#endif
		infomsg("allocating memory for %i blocks with %i byte (%i kB total)...\n",Numblocks,Blocksize,Numblocks*Blocksize/1024);
		Buffer[0] = (char *) malloc(Blocksize * Numblocks);
#ifdef HAVE_MMAP
	}
#endif
	for (c = 1; c < Numblocks; c++)
		Buffer[c] = Buffer[0] + Blocksize * c;

	debugmsg("creating semaphore...\n");
	if (0 != sem_init(&Buf2Dev,0,0))
		perror("Error creating semaphore");
	if (0 != sem_init(&Dev2Buf,0,Numblocks))
		perror("Error creating semaphore");

	debugmsg("opening streams...\n");
	if (inFile) {
		if (-1 == (In = open(inFile,O_RDONLY)))
			fatal("could not open input file: %s\n",strerror(errno));
	} else
		In = fileno(stdin);
	if (outFile) {
		if (-1 == (Out = open(outFile,nooverwrite|O_CREAT|O_WRONLY|O_TRUNC,0666)))
			fatal("could not open output file: %s\n",strerror(errno));
	} else
		Out = fileno(stdout);

	debugmsg("registering signals...\n");
	signal(SIGINT,sigHandler);
	signal(SIGTERM,sigHandler);

	debugmsg("starting threads...\n");
	pthread_create(&Reader,0,(void *(*)(void *))&inputThread,0);
	pthread_create(&Writer,0,(void *(*)(void *))&outputThread,0);
	if (Verbose >= 3)
		statusThread();
	else {
		pthread_join(Reader,0);
		pthread_join(Writer,0);
	}
	return 0;
}
