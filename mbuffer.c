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
#include <sys/stat.h>
#include <termios.h>
#include <unistd.h>

#ifdef EXPERIMENTAL
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>

int Sock = 0;
#endif

pthread_t Reader, Writer;
int Verbose = 3, Finish = 0, In, Out, Tmp, Rest = 0, Pause = 0, 
	Memmap = 0, Status = 1, Outsize = 0, Nooverwrite = O_EXCL, 
	Numblocks = 256;
unsigned long long Blocksize = 10240, Numin = 0, Numout = 0;
float Start = 0;
char *Tmpfile = 0, **Buffer;
const char *Infile = 0, *Outfile = 0;
#ifdef MULTIVOLUME
int Multivolume = 0;
#endif
sem_t Dev2Buf,Buf2Dev,Percentage;
FILE *Log = 0, *Terminal = 0;
struct timeb Starttime;


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

void warningmsg(const char *msg, ...)
{
	va_list val;
	if (Verbose < 3)
		return;
	va_start(val,msg);
	fprintf(Log,"warning: ");
	vfprintf(Log,msg,val);
	va_end(val);
}

void errormsg(const char *msg, ...)
{
	va_list val;

	if (Verbose < 2)
		return;
	va_start(val,msg);
	fprintf(Log,"error: ");
	vfprintf(Log,msg,val);
	va_end(val);
}


void fatal(const char *msg, ...)
{
	va_list val;

	if (Verbose < 1)
		return;
	va_start(val,msg);
	fprintf(Log,"fatal: ");
	vfprintf(Log,msg,val);
	va_end(val);
	exit(-1);
}

void terminate()
{
	float diff,out;
	struct timeb now;

	infomsg("\rterminating...\n");
	pthread_cancel(Reader);
	pthread_cancel(Writer);

	pthread_join(Reader,0);
	pthread_join(Writer,0);
	if (Memmap)
		munmap(Buffer[0],Blocksize*Numblocks);
#ifdef EXPERIMENTAL
	if (Sock)
		close(Sock);
#endif
	close(Tmp);
	remove(Tmpfile);
	if (Status) {
		ftime(&now);
		diff = now.time - Starttime.time + (float) now.millitm / 1000 - (float) Starttime.millitm / 1000;
		out = (float)(((long long) Numout * Blocksize) >> 10) / diff;
		fprintf(Terminal,"\nsummary: %Lu kB in %.1f sec - %.1f kB/sec average\n",
			(Numout * Blocksize) >> 10,
			diff, out );
	}
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
	struct timeb last, now;
	float in = 0, out = 0, diff, fill;
	unsigned long long total, lin = 0, lout = 0;
	int rest;

	ftime(&Starttime);
	last.time = Starttime.time;
	last.millitm = Starttime.millitm;
	usleep(1000);	/* needed on alpha (stderr fails with fpe on nan) */
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
		total = (Numout * Blocksize) >> 10;
		fill = (fill < 0) ? 0 : fill;
		fprintf(Terminal,"\rin at %8.1f kB/sec - out at %8.1f kB/sec - %Lu kB totally transfered - buffer %3.0f%% full",in,out,total,fill);
		fflush(Terminal);
		usleep(500000);
		sem_getvalue(&Buf2Dev,&rest);
	}
	fprintf(stderr,"\n");
	infomsg("statusThread: joining to terminate...\n");
	pthread_join(Reader,0);
	pthread_join(Writer,0);
	if (Memmap)
		munmap(Buffer[0],Blocksize*Numblocks);
	close(Tmp);
	remove(Tmpfile);
	ftime(&now);
	diff = now.time - Starttime.time + (float) now.millitm / 1000 - (float) Starttime.millitm / 1000;
	out = (float)(((long long) Numout * Blocksize) >> 10) / diff;
	fprintf(Terminal,"summary: %Lu kB in %.1f sec - %.1f kB/sec average\n",
		(Numout * Blocksize) >> 10,
		diff, out );
	exit(0);
}

#ifdef MULTIVOLUME
void requestInputVolume()
{
	close(In);
	do {
		fprintf(Terminal,"\ninsert next volume...");
		fflush(Terminal);
		tcflush(fileno(Terminal),TCIFLUSH);
		fgetc(Terminal);
		if (-1 == (In = open(Infile,O_RDONLY)))
			errormsg("could not reopen input: %s\n",strerror(errno));
	} while (In == -1);
	Multivolume--;
	fprintf(Terminal,"\nOK - continuing...");
}
#endif

void inputThread()
{
	int at = 0, err, num, perc, fill;

	infomsg("inputThread: starting...\n");
	pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS,0);
	while (!Finish) {
		debugmsg("inputThread: wait\n");
		sem_wait(&Dev2Buf);
		num = 0;
		do {
			debugmsg("inputThread: read %i\n",num);
			err = read(In,Buffer[at] + num,Blocksize - num);
#ifdef MULTIVOLUME
			if ((!err) && (Terminal) && (Multivolume)) {
				requestInputVolume();
			} else
#endif
			if (-1 == err) {
				errormsg("inputThread: error reading: %s\n",strerror(errno));
				sem_post(&Buf2Dev);
				sem_post(&Percentage);
				Finish = 1;
				infomsg("inputThread: exiting...\n");
				pthread_exit((void *) 0);
			} else if (0 == err) {
				Finish = 1;
				Rest = num;
				debugmsg("inputThread: last block has %i bytes\n",Rest);
				sem_post(&Buf2Dev);
				sem_post(&Percentage);
				infomsg("inputThread: exiting...\n");
				pthread_exit(0);
			} 
			num += err;
		} while (num < Blocksize);
		debugmsg("inputThread: post\n");
		sem_post(&Buf2Dev);
		sem_getvalue(&Buf2Dev,&fill);
		if (((float) fill / (float) Numblocks) >= Start) {
			sem_getvalue(&Percentage,&perc);
			if (!perc) {
				infomsg("\ninputThread: percentage reached - restarting output...\n");
				sem_post(&Percentage);
			}
		}
		at++;
		if (at == Numblocks)
			at = 0;
		Numin++;
	}
	sem_post(&Percentage);
	infomsg("inputThread: exiting...");
}

#ifdef MULTIVOLUME
void requestOutputVolume()
{
	if (!Outfile) {
		errormsg("End of volume, but not end of input:\n"
			"Output file must be given (option -o) for multi volume support!\n");
		Finish = 1;
		pthread_exit((void *) -1);
	}
	close(Out);
	fprintf(Terminal,"\nvolume full - insert new media and press return whe ready...\n");
	tcflush(fileno(Terminal),TCIFLUSH);
	fgetc(Terminal);
	fprintf(Terminal,"\nOK - continuing...\n");
	if (-1 == (Out = open(Outfile,Nooverwrite|O_CREAT|O_WRONLY|O_TRUNC|O_SYNC,0666))) {
		errormsg("error reopening output file: %s\n",strerror(errno));
		Finish = 1;
		pthread_exit((void *) -1);
	}
}
#endif

void outputThread()
{
	int at = 0, err, fill, num, rest;
	
	infomsg("\noutputThread: starting...\n");
	pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS,0);
	while (1) {
		debugmsg("outputThread: wait\n");
		sem_getvalue(&Buf2Dev,&fill);
		if (Start && (!fill))
			sem_wait(&Percentage);
		sem_wait(&Buf2Dev);
		num = 0;
		if (Finish) {
			debugmsg("outputThread: inputThread finished, %i blocks remaining\n",fill);
			sem_getvalue(&Buf2Dev,&fill);
			if ((0 == Rest) && (0 == fill)) {
				infomsg("outputThread: finished - exiting...\n");
				pthread_exit((void *) 0);
			} else if (0 == fill) {
				debugmsg("outputThread: last block has %i bytes\n",Rest);
				Blocksize = Rest;
			}
		}
		rest = Blocksize;
		do {
			debugmsg("outputThread: write %i\n",-num);
			/* use Outsize which is the blocksize of the device */
			err = write(Out,Buffer[at] + num, rest > Outsize ? Outsize : rest );
#ifdef MULTIVOLUME
			if ((-1 == err) && (Terminal) && ((errno == ENOMEM) || (errno == ENOSPC))) {
				requestOutputVolume();
				continue;
			} else if (-1 == err) {
#else
			if (-1 == err) {
#endif
				errormsg("outputThread: error writing: %s\n",strerror(errno));
				Finish = 1;
				pthread_exit((void *) -1);
			}
			rest -= err;
			if (Pause)
				usleep(Pause);
		} while (rest > 0);
		at++;
		if (Finish && (0 == fill)) {
			infomsg("syncing...\n");
			fsync(Out);
			infomsg("outputThread: finished - exiting...\n");
			close(Out);
			pthread_exit(0);
		}
		debugmsg("outputThread: post\n");
		sem_post(&Dev2Buf);
		if (Numblocks == at)
			at = 0;
		Numout++;
	}
}

#ifdef EXPERIMENTAL

void openNetworkInput(unsigned short port)
{
	struct sockaddr_in saddr, caddr;
	socklen_t clen;

	infomsg("creating socket for network input...\n");
	Sock = socket(AF_INET, SOCK_STREAM, 6);
	if (0 > Sock)
		fatal("could not crate socket for network input: %s\n",strerror(errno));
	bzero(&saddr, sizeof(saddr));
	saddr.sin_family = AF_INET;
	saddr.sin_addr.s_addr = htonl(INADDR_ANY);
	saddr.sin_port = htons(port);
	infomsg("binding socket...\n");
	if (0 > bind(Sock, (struct sockaddr *) &saddr, sizeof(saddr)))
		fatal("could not bind to socket for network input: %s\n",strerror(errno));
	infomsg("listening on socket...\n");
	if (0 > listen(Sock,1))		/* accept only 1 incoming connection */
		fatal("could not listen on socket for network input: %s\n",strerror(errno));
	infomsg("waiting to accept connection...\n");
	In = accept(Sock, (struct sockaddr *) &caddr, &clen);
	if (0 > In)
		fatal("could not accept connection for network input: %s\n",strerror(errno));
}

void openNetworkOutput(const char *host, unsigned short port)
{
	struct sockaddr_in saddr;

	infomsg("creating socket for network output...\n");
	Out = socket(AF_INET, SOCK_STREAM, 6);
	if (0 > Out)
		fatal("could not crate socket for network output: %s\n",strerror(errno));
	bzero(&saddr, sizeof(saddr));
	saddr.sin_family = AF_INET;
	saddr.sin_port = htons(port);
	infomsg("resolving server host...\n");
	if (0 > inet_pton(AF_INET, host, &saddr.sin_addr))
		fatal("could not resolve server hostname: %s\n",strerror(errno));
	infomsg("connecting to server (%x)...\n",saddr.sin_addr);
	if (0 > connect(Out, (struct sockaddr *) &saddr, sizeof(saddr)))
		fatal("could not connect to server: %s\n",strerror(errno));
}

void getNetVars(const char **argv, int *c, const char **server, unsigned short *port)
{
	char *tmpserv;
	
	tmpserv = malloc(strlen(argv[*c] + 1));
	if (0 == tmpserv)
		fatal("out of memory in getNetVars(...)\n");
	if (1 < sscanf(argv[*c],"%[0-9a-zA-Z.]:%hu",tmpserv,port)) {
		*server = tmpserv;
		return;
	}
	free((void *) tmpserv);
	*server = argv[*c];
	(*c)++;
	*port = atoi(argv[*c]);
	if (*port)
		(*c)++;
}

#endif

void version()
{
	fprintf(stderr,
		"mbuffer version "VERSION"\n"\
		"Copyright 2001 - T. Maier-Komor\n"\
		"License: GPL2 - see file COPYING\n");
	exit(0);
}

void usage()
{
	fprintf(stderr,
		"mbuffer [Options]\n\n"
		"Options:\n"
		"-b <num>   : use <num> blocks for buffer (default %i)\n"
		"-s <size>  : use block of <size> bytes for buffer (default %Lu)\n"
		"-m <size>  : use buffer of a total of <size> bytes\n"
#ifdef HAVE_MMAP
		"-t         : use memory mapped temporary file (for huge buffer)\n"
#endif
		"-p <num>   : start writing after buffer has been filled <num>%%\n"
		"-i <file>  : use <file> for input\n"
#ifdef EXPERIMENTAL
		"-I <port>  : use network port <port> as input\n"
#endif
		"-o <file>  : use <file> for output\n"
#ifdef MULTIVOLUME
		"-n <num>   : <num> volumes for input\n"
#endif
		"-T <file>  : as -t but uses <file> as buffer\n"
		"-l <file>  : use <file> for logging messages\n"
		"-u <num>   : pause <num> microseconds after each write\n"
		"-f         : overwrite existing files\n"
		"-v <level> : set verbose level to <level> (valid values are 0..5)\n"
		"-q         : quiet - do not display the status on stderr\n"
		"--version  : print version information\n\n"
		"Unsupported buffer options: -t -Z -B\n",
		Numblocks,Blocksize);
	exit(0);
}

unsigned long long calcint(const char **argv, int c, unsigned long long d)
{
	char ch;
	unsigned long long i;
	
	switch (sscanf(argv[c],"%Lu%c",&i,&ch)) {
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
			fatal("unrecognized size charakter '%c' in option\n",c);
			return d;
		}
	case 1:
		return i;
	}
	errormsg("unrecognized argument \"%s\" for option \"%s\"\n",argv[c],argv[c-1]);
	return d;
}

int argcheck(const char *opt, const char **argv, int *c)
{
	if (strncmp(opt,argv[*c],2))
		return 1;
	if (strlen(argv[*c]) > 2)
		argv[*c] += 2;
	else
		(*c)++;
	return 0;
}

int main(int argc, const char **argv)
{
	unsigned long long totalmem = 0;
	int c, optMset = 0, optSset = 0, optBset = 0;
#ifdef HAVE_ST_BLKSIZE
	struct stat st;
#endif
#ifdef EXPERIMENTAL
	unsigned short netPortIn = 0;
	const char *server = 0;
	unsigned short netPortOut = 0;
#endif
	
	Log = stderr;
	for (c = 1; c < argc; c++) {
		if (!argcheck("-s",argv,&c)) {
			Blocksize = calcint(argv,c,Blocksize);
			optSset = 1;
			debugmsg("Blocksize set to %Lu\n",Blocksize);
		} else if (!argcheck("-m",argv,&c)) {
			totalmem = calcint(argv,c,totalmem);
			optMset = 1;
			debugmsg("totalmem set to %Lu\n",totalmem);
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
#ifdef MULTIVOLUME
		} else if (!argcheck("-n",argv,&c)) {
			Multivolume = atoi(argv[c]) - 1;
			if (Multivolume <= 0)
				fatal("argument for number of volumes must be > 0\n");
			debugmsg("Multivolume set to %i\n",Multivolume);
#endif
		} else if (!argcheck("-i",argv,&c)) {
			Infile = argv[c];
			debugmsg("Infile set to %s\n",Infile);
#ifdef EXPERIMENTAL
		} else if (!argcheck("-I",argv,&c)) {
			netPortIn = (atoi(argv[c])) ? (atoi(argv[c])): 0;
			debugmsg("Network input set to port %hu\n",netPortIn);
#endif
		} else if (!argcheck("-o",argv,&c)) {
			Outfile = argv[c];
			debugmsg("Outfile set to %s\n",Outfile);
#ifdef EXPERIMENTAL
		} else if (!argcheck("-O",argv,&c)) {
			getNetVars(argv,&c,&server,&netPortOut);
			debugmsg("Output: server = %s, port = %hu\n",server,netPortOut);
#endif
		} else if (!argcheck("-T",argv,&c)) {
			Tmpfile = malloc(strlen(argv[c]) + 1);
			if (!Tmpfile)
				fatal("out of memory");
			strcpy(Tmpfile, argv[c]);
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
		} else if (!strcmp("-t",argv[c])) {
			Memmap = 1;
			debugmsg("mm set to 1\n");
#endif
		} else if (!strcmp("-f",argv[c])) {
			Nooverwrite = 0;
			debugmsg("overwrite set to 0\n");
		} else if (!strcmp("-q",argv[c])) {
			debugmsg("disabling display of status\n");
			Status = 0;
		} else if (!argcheck("-p",argv,&c)) {
			if (1 != sscanf(argv[c],"%f",&Start))
				Start = 0;
			Start /= 100;
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
		if ((totalmem / Blocksize) >= (1 << 31))
			fatal("maximum number of managable blocks is %Li\n"
				"Try a bigger blocksize!\n",1 << 31);
		Numblocks = totalmem / Blocksize;
		infomsg("Numblocks set to %i\n",Numblocks);
	} else if (optBset&!optSset&optMset) {
		Blocksize = totalmem / Numblocks;
		infomsg("blocksize set to %Lu\n",Blocksize);
	}
#ifdef EXPERIMENTAL
	if (Infile && netPortIn)
		fatal("Setting both network input port and input file doesn't make sense!");
	if ((netPortOut == 0) ^ (server == 0))
		fatal("When sending data to a server, both servername and port must be set!\n");
#endif

#ifdef MULTIVOLUME
	/* multi volume input consistency checking */
	if ((Multivolume) && (!Infile))
		fatal("multi volume support for input needs an explicit given input device (option -i)\n");
#endif

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
			Tmp = mkstemp(Tmpfile);
		} else {
			Tmp = open(Tmpfile,O_RDWR|O_CREAT|O_EXCL);
		}
		if (-1 == Tmp)
			fatal("could not create temporary file (%s): %s\n",Tmpfile,strerror(errno));
		/* resize the file. Needed - at least under linux, who knows why? */
		if (-1 == lseek(Tmp,Numblocks*Blocksize-sizeof(int),SEEK_SET))
			fatal("could not resize temporary file: %s\n",strerror(errno));
		if (-1 == write(Tmp,&c,sizeof(int)))
			fatal("could not resize temporary file: %s\n",strerror(errno));
		Buffer[0] = mmap(0,Blocksize*Numblocks,PROT_READ|PROT_WRITE,MAP_PRIVATE,Tmp,0);
		if (MAP_FAILED == Buffer[0])
			fatal("could not map buffer-file to memory: %s\n",strerror(errno));
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
		fatal("Error creating semaphore: %s\n",strerror(errno));
	if (0 != sem_init(&Dev2Buf,0,Numblocks))
		fatal("Error creating semaphore: %s\n",strerror(errno));
	if (0 != sem_init(&Percentage,0,0))
		fatal("Error creating semaphore: %s\n",strerror(errno));

	debugmsg("opening streams...\n");
	if (Infile) {
		if (-1 == (In = open(Infile,O_RDONLY)))
			fatal("could not open input file: %s\n",strerror(errno));
#ifdef EXPERIMENTAL
	} else if (netPortIn) {
		openNetworkInput(netPortIn);
#endif
	} else
		In = fileno(stdin);
	if (Outfile) {
		if (-1 == (Out = open(Outfile,Nooverwrite|O_CREAT|O_WRONLY|O_TRUNC|O_SYNC,0666)))
			fatal("could not open output file: %s\n",strerror(errno));
#ifdef EXPERIMENTAL
	} else if (netPortOut) {
		openNetworkOutput(server,netPortOut);
#endif
	} else
		Out = fileno(stdout);

#ifdef HAVE_ST_BLKSIZE
	debugmsg("checking blocksize for output...\n");
	if (-1 == fstat(Out,&st))
		fatal("could not stat output: %s\n",strerror(errno));
	if ((st.st_mode & S_IFBLK) || (st.st_mode & S_IFCHR)) {
		infomsg("blocksize on output device is %i\n",st.st_blksize);
		if (Blocksize%st.st_blksize != 0)
			warningmsg("Blocksize should be a multiple of the blocksize of the output device (is %i)!\n",st.st_blksize);
		if (Blocksize != st.st_blksize)
			infomsg("setting output blocksize to %i\n",st.st_blksize);
		Outsize = st.st_blksize;
	} else
		infomsg("no device on output stream\n");
#endif

	if (Status) {
		debugmsg("accessing terminal...\n");
		Terminal = fopen("/dev/tty","r+");
		if (!Terminal) {
			errormsg("could not open terminal: %s\n",strerror(errno));
			warningmsg("no multi volume support");
			Status = 0;
		}
	}

	debugmsg("registering signals...\n");
	if (SIG_ERR == signal(SIGINT,sigHandler))
		warningmsg("error registering new SIGINT handler: %s\n",strerror(errno));
	if (SIG_ERR == signal(SIGTERM,sigHandler))
		warningmsg("error registering new SIGINT handler: %s\n",strerror(errno));

	debugmsg("starting threads...\n");
	pthread_create(&Reader,0,(void *(*)(void *))&inputThread,0);
	pthread_create(&Writer,0,(void *(*)(void *))&outputThread,0);
	if (Status)
		statusThread();
	else {
		pthread_join(Reader,0);
		pthread_join(Writer,0);
	}
	return 0;
}
