#include "config.h"
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>
#include <sys/timeb.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <termios.h>
#include <unistd.h>

#ifdef MHASH
#include <mhash.h>
#elif defined MD5
#include <md5.h>
#endif

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>



pthread_t Reader, Writer;
long Verbose = 3, In, Out, Tmp, Pause = 0, Memmap = 0, Status = 1, 
	Outsize = 10240, Nooverwrite = O_EXCL, Outblocksize = 0,
	Autoloader = 0, Hash = 0, Sock = 0;
volatile int Rest = 0, Finish = 0;
unsigned long long Blocksize = 10240, Numblocks = 256;
volatile unsigned long long Numin = 0, Numout = 0;
float StartWrite = 0, StartRead = 1;
char *Tmpfile = 0, **Buffer;
const char *Infile = 0, *Outfile = 0, *Tape = 0;
int Multivolume = 0;
#ifdef MHASH
MHASH MD5hash;
#elif defined MD5
MD5_CTX md5ctxt;
#endif
sem_t Dev2Buf,Buf2Dev,PercentageHigh,PercentageLow;
pthread_mutex_t TermMut;
FILE *Log = 0, *Terminal = 0;
struct timeb Starttime;


#ifdef DEBUG
void debugmsg(const char *msg, ...)
{
	if (Verbose >= 5) {
		va_list val;
		va_start(val,msg);
		vfprintf(Log,msg,val);
		va_end(val);
	}
}
#else
#define debugmsg(...)
#endif

void infomsg(const char *msg, ...)
{
	if (Verbose >= 4) {
		va_list val;
		va_start(val,msg);
		vfprintf(Log,msg,val);
		va_end(val);
	}
}

void warningmsg(const char *msg, ...)
{
	if (Verbose >= 3) {
		va_list val;
		va_start(val,msg);
		fprintf(Log,"warning: ");
		vfprintf(Log,msg,val);
		va_end(val);
	}
}

void errormsg(const char *msg, ...)
{
	if (Verbose >= 2) {
		va_list val;
		va_start(val,msg);
		fprintf(Log,"error: ");
		vfprintf(Log,msg,val);
		va_end(val);
	}
}


void fatal(const char *msg, ...)
{
	if (Verbose >= 1) {
		va_list val;
		va_start(val,msg);
		fprintf(Log,"fatal: ");
		vfprintf(Log,msg,val);
		va_end(val);
	}
	exit(-1);
}

void summary(unsigned long long numb, float secs)
{
	int h = (int) secs/3600, m = (int) secs/60;
	double av = (double)numb/secs;
	
	fprintf(Terminal,"\nsummary: ");
	if (numb < 1331ULL)			/* 1.3 kB */
		fprintf(Terminal,"%llu Byte in ",numb);
	else if (numb < 1363149ULL)		/* 1.3 MB */
		fprintf(Terminal,"%.1f kB in ",(float)numb / 1024);
	else if (numb < 1395864371ULL)		/* 1.3 GB */
		fprintf(Terminal,"%.1f MB in ",(float)numb / (float)(1<<20));
	else
		fprintf(Terminal,"%.1f GB in ",(float)numb / (1<<30));
	if (h)
		fprintf(Terminal,"%d h ",h);
	if (m)
		fprintf(Terminal,"%02d min ",m);
	fprintf(Terminal,"%02.1f sec - ",secs);
	fprintf(Terminal,"average of ");
	if (av < 1331)
		fprintf(Terminal,"%.0f B/s\n",av);
	else if (av < 1363149)			/* 1.3 MB */
		fprintf(Terminal,"%.1f kB/s\n",av/1024);
	else if (av < 1395864371)		/* 1.3 GB */
		fprintf(Terminal,"%.1f MB/s\n",av/1048576);
	else
		fprintf(Terminal,"%.1f GB/s\n",av/1073741824);	/* OK - this is really silly - at least now in 2003, yeah and still in 2005... */
#ifdef MHASH
	if (Hash) {
		unsigned char hashvalue[16];
		int i;
		
		mhash_deinit(MD5hash,hashvalue);
		fprintf(Terminal,"md5 hash:");
		for (i = 0; i < 16; ++i)
			fprintf(Terminal," %02x",hashvalue[i]);
		fprintf(Terminal,"\n");
	}
#elif defined MD5
	if (Hash) {
		unsigned char hashvalue[16];
		int i;
		
		MD5Final(hashvalue,&md5ctxt);
		fprintf(Terminal,"md5 hash:");
		for (i = 0; i < 16; ++i)
			fprintf(Terminal," %02x",hashvalue[i]);
		fprintf(Terminal,"\n");
	}
#endif
}

void terminate(void)
{
	float diff;
	struct timeb now;

	infomsg("\rterminating...\n");
	pthread_cancel(Reader);
	pthread_cancel(Writer);

	pthread_join(Reader,0);
	pthread_join(Writer,0);
	if (Memmap)
		munmap(Buffer[0],Blocksize*Numblocks);
	if (Sock)
		close(Sock);
	close(Tmp);
	remove(Tmpfile);
	if (Status) {
		ftime(&now);
		diff = now.time - Starttime.time + (float) now.millitm / 1000 - (float) Starttime.millitm / 1000;
		summary(Numout * Blocksize + Rest,diff);
	}
	exit(0);
}

RETSIGTYPE sigHandler(int signr)
{
	switch (signr) {
	case SIGINT:
		infomsg("\rcatched INT signal...\n");
		terminate();
		break;
	case SIGTERM:
		infomsg("\rcatched TERM signal...\n");
		terminate();
		break;
	default:
		debugmsg("\rcatched unexpected signal %d...\n",signr);
	}
}

void statusThread(void) 
{
	struct timeb last, now;
	float in = 0, out = 0, diff, fill;
	unsigned long long total, lin = 0, lout = 0;
	int unwritten;

	ftime(&Starttime);
	last.time = Starttime.time;
	last.millitm = Starttime.millitm;
	usleep(1000);	/* needed on alpha (stderr fails with fpe on nan) */
	while (!(Finish && (unwritten == 0))) {
		int err;
		sem_getvalue(&Buf2Dev,&unwritten);
		fill = (float)unwritten / (float)Numblocks * 100.0;
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
		err = pthread_mutex_lock(&TermMut);
		assert(0 == err);
		fprintf(Terminal,"\r%8.1f kB/s in - %8.1f kB/s out - %llu kB total - buffer %3.0f%% full",in,out,total,fill);
		fflush(Terminal);
		err = pthread_mutex_unlock(&TermMut);
		assert(0 == err);
		usleep(500000);
	}
	fprintf(Terminal,"\n");
	infomsg("statusThread: joining to terminate...\n");
	pthread_join(Reader,0);
	pthread_join(Writer,0);
	if (Memmap)
		munmap(Buffer[0],Blocksize*Numblocks);
	close(Tmp);
	remove(Tmpfile);
	ftime(&now);
	diff = now.time - Starttime.time + (float) now.millitm / 1000.0 - (float) Starttime.millitm / 1000.0;
	summary(Numout * Blocksize + Rest, diff);
	exit(0);
}

void requestInputVolume(void)
{
	char cmd[15+strlen(Infile)];
	int err;

	debugmsg("requesting new volume for input\n");
	close(In);
	do {
		if ((Autoloader) && (Infile)) {
			infomsg("requesting change of volume...\n");
			sprintf(cmd,"mt -f %s offline",Infile);
			if (-1 == system(cmd)) {
				warningmsg("error executing mt to change volume in autoloader...\n");
				Autoloader = 0;
				continue;
			}
			infomsg("waiting for drive to get ready...\n");
			sleep(Autoloader);
		} else {
			int err1;
			err1 = pthread_mutex_lock(&TermMut);
			assert(0 == err1);
			fprintf(Terminal,"\ninsert next volume and press return to continue...");
			fflush(Terminal);
			tcflush(fileno(Terminal),TCIFLUSH);
			fgetc(Terminal);
			err1 = pthread_mutex_unlock(&TermMut);
			assert(0 == err1);
		}
		if (-1 == (In = open(Infile,O_RDONLY)))
			errormsg("could not reopen input: %s\n",strerror(errno));
	} while (In == -1);
	Multivolume--;
	err = pthread_mutex_lock(&TermMut);
	assert(0 == err);
	fprintf(Terminal,"\nOK - continuing...");
	fflush(Terminal);
	err = pthread_mutex_unlock(&TermMut);
	assert(0 == err);
}

void *inputThread(void *ignored)
{
	int err, fill = 0;
	unsigned long long num, at = 0;
	const float startread = StartRead, startwrite = StartWrite;

	infomsg("inputThread: starting...\n");
	pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS,0);
	while (1) {
		if ((startread < 1) && (fill == Numblocks - 1)) {
			debugmsg("inputThread: buffer full, waiting for it to drain.\n");
			sem_wait(&PercentageLow);
		}
		debugmsg("inputThread: sem_wait\n");
		sem_wait(&Dev2Buf); // Wait for one or more buffer blocks to be free
		num = 0;
		do {
			err = read(In,Buffer[at] + num,Blocksize - num);
			debugmsg("inputThread: read(In, Buffer[%d] + %d, %llu) = %d\n", at, num, Blocksize - num, err);
			if ((!err) && (Terminal) && (Multivolume)) {
				requestInputVolume();
			} else if (-1 == err) {
				errormsg("inputThread: error reading: %s\n",strerror(errno));
				if (num) {
					Rest = num;
					sem_post(&Buf2Dev);
				}
				sem_post(&PercentageLow);
				Finish = 1;
				infomsg("inputThread: exiting...\n");
				pthread_exit((void *) 0);
			} else if (0 == err) {
				Rest = num;
				Finish = 1;
				debugmsg("inputThread: last block has %d bytes\n",num);
				#ifdef MHASH
				if (Hash)
					mhash(MD5hash,Buffer[at],num);
				#elif defined MD5
				if (Hash)
					MD5Update(&md5ctxt,Buffer[at],num);
				#endif
				sem_post(&Buf2Dev);
				sem_post(&PercentageHigh);
				infomsg("inputThread: exiting...\n");
				pthread_exit(0);
			} 
			num += err;
		} while (num < Blocksize);
		debugmsg("inputThread: sem_post\n");
		#ifdef MHASH
		if (Hash)
			mhash(MD5hash,Buffer[at],Blocksize);
		#elif defined MD5
		if (Hash)
			MD5Update(&md5ctxt,Buffer[at],num);
		#endif
		sem_post(&Buf2Dev);
		sem_getvalue(&Buf2Dev,&fill);
		if (((float) fill / (float) Numblocks) >= startwrite) {
			int perc;
			sem_getvalue(&PercentageHigh,&perc);
			if (!perc) {
				infomsg("\ninputThread: high watermark reached - restarting output...\n");
				sem_post(&PercentageHigh);
			}
		}
		at++;
		if (at == Numblocks)
			at = 0;
		Numin++;
	}
}

void requestOutputVolume(void)
{
	char cmd[15+strlen(Outfile)];

	if (!Outfile) {
		errormsg("End of volume, but not end of input:\n"
			"Output file must be given (option -o) for multi volume support!\n");
		Finish = 1;
		pthread_exit((void *) -1);
	}
	debugmsg("requesting new output volume\n");
	close(Out);
	do {
		if ((Autoloader) && (Outfile)) {
			infomsg("requesting change of volume...\n");
			sprintf(cmd,"mt -f %s offline",Outfile);
			if (-1 == system(cmd)) {
				warningmsg("error executing mt to change volume in autoloader...\n");
				Autoloader = 0;
				continue;
			}
			infomsg("waiting for drive to get ready...\n");
			sleep(Autoloader);
		} else {
			int err;
			err = pthread_mutex_lock(&TermMut);
			assert(0 == err);
			fprintf(Terminal,"\nvolume full - insert new media and press return whe ready...\n");
			tcflush(fileno(Terminal),TCIFLUSH);
			fgetc(Terminal);
			fprintf(Terminal,"\nOK - continuing...\n");
			err = pthread_mutex_unlock(&TermMut);
			assert(0 == err);
		}
		if (-1 == (Out = open(Outfile,Nooverwrite|O_CREAT|O_WRONLY|O_TRUNC|O_SYNC,0666)))
			errormsg("error reopening output file: %s\n",strerror(errno));
	} while (-1 == Out);
	infomsg("continuing with next volume\n");
}

void checkIncompleteOutput(void)
{
	static int mulretry = 0;	/* well this isn't really good design,
					   but better than a global variable */
	
	debugmsg("Outblocksize = %d, mulretry = %d\n",Outblocksize,mulretry);
	if ((0 != mulretry) || (0 == Outblocksize)) {
		requestOutputVolume();
		debugmsg("resetting outputsize to normal\n");
		if (0 != mulretry) {
			Outsize = mulretry;
			mulretry = 0;
		}
	} else {
		debugmsg("setting to new outputsize (end of device)\n");
		mulretry = Outsize;
		Outsize = Outblocksize;
	}
}

void *outputThread(void *ignored)
{
	int at = 0, fill;
	const float startwrite = StartWrite, startread = StartRead;
	unsigned long long blocksize = Blocksize;
	
	infomsg("\noutputThread: starting...\n");
	pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS,0);
	while (1) {
		int rest = blocksize;
		debugmsg("outputThread: sem_wait\n");
		sem_getvalue(&Buf2Dev,&fill);
		if ((fill == 0) && (startwrite > 0)) {
			debugmsg("outputThread: buffer empty, waiting for it to fill\n");
			sem_wait(&PercentageHigh);
		}
		sem_wait(&Buf2Dev);
		if (Finish) {
			debugmsg("outputThread: inputThread finished, %d blocks remaining\n",fill);
			sem_getvalue(&Buf2Dev,&fill);
			if ((0 == Rest) && (0 == fill)) {
				infomsg("outputThread: finished - exiting...\n");
				pthread_exit((void *) 0);
			} else if (0 == fill) {
				blocksize = rest = Rest;
				debugmsg("outputThread: last block has %d bytes\n",Rest);
			}
		}
		do {
			/* use Outsize which could be the blocksize of the device (option -d) */
			int err = write(Out,Buffer[at] + blocksize - rest, rest > Outsize ? Outsize : rest);
			debugmsg("outputThread: write(Out, Buffer[%d] + %llu, %d) = %d\t(rest = %d)\n", at, blocksize - rest, rest > Outsize ? Outsize : rest, err, rest);
			if ((-1 == err) && (Terminal) && ((errno == ENOMEM) || (errno == ENOSPC))) {
				/* request a new volume - but first check
				 * wheather we are really at the
				 * end of the device */
				checkIncompleteOutput();
				continue;
			} else if (-1 == err) {
				errormsg("outputThread: error writing: %s\n",strerror(errno));
#ifdef DEBUG
				errormsg("outputThread debug info: write(Out, Buffer[%d] + %llu, %d) = %d\t(rest = %d)\n", at, blocksize - rest, rest > Outsize ? Outsize : rest, err, rest);
#endif
				Finish = 1;
				sem_post(&Dev2Buf);
				pthread_exit((void *) -1);
			}
			rest -= err;
			if (Pause)
				usleep(Pause);
		} while (rest > 0);
		debugmsg("outputThread: sem_post\n");
		sem_post(&Dev2Buf);
		at++;
		if (Numblocks == at)
			at = 0;
		if (Finish) {
			sem_getvalue(&Buf2Dev,&fill);
			if (0 == fill) {
				infomsg("syncing...\n");
				fsync(Out);
				infomsg("outputThread: finished - exiting...\n");
				close(Out);
				pthread_exit(0);
			}
		}
		sem_getvalue(&Buf2Dev,&fill);
		if ((startread < 1) && ((float)fill / (float)Numblocks) < startread) {
			int perc;
			sem_getvalue(&PercentageLow,&perc);
			if (!perc) {
				infomsg("\noutputThread: low watermark reached - restarting input...\n");
				sem_post(&PercentageLow);
			}
		}
		Numout++;
	}
}



void openNetworkInput(const char *host, unsigned short port)
{
	struct sockaddr_in saddr, caddr;
	size_t clen;
	struct hostent *h = 0, *r = 0;
	char *rhost = 0;

	debugmsg("openNetworkInput(%s,%hu)\n",host,port);
	infomsg("creating socket for network input...\n");
	Sock = socket(AF_INET, SOCK_STREAM, 6);
	if (0 > Sock)
		fatal("could not create socket for network input: %s\n",strerror(errno));
	bzero((void *) &saddr, sizeof(saddr));
	if (host) {
		debugmsg("resolving hostname of input interface...\n");
		if (0 == (h = gethostbyname(host)))
			fatal("could not resolve server hostname: %s\n",hstrerror(h_errno));
	}
	saddr.sin_family = AF_INET;
	saddr.sin_addr.s_addr = htonl(INADDR_ANY);
	saddr.sin_port = htons(port);
	infomsg("binding socket...\n");
	if (0 > bind(Sock, (struct sockaddr *) &saddr, sizeof(saddr)))
		fatal("could not bind to socket for network input: %s\n",strerror(errno));
	infomsg("listening on socket...\n");
	if (0 > listen(Sock,1))		/* accept only 1 incoming connection */
		fatal("could not listen on socket for network input: %s\n",strerror(errno));
	do {
		if (0 < In) {
			close(In);
			rhost = inet_ntoa(caddr.sin_addr);
			r = gethostbyaddr(rhost,strlen(rhost),AF_INET);
			if (r)
				errormsg("rejected connection from %s (%s)\n",r->h_name,rhost);
			else
				errormsg("rejected connection from %s\n",rhost);
		}
		infomsg("waiting to accept connection...\n");
		In = accept(Sock, (struct sockaddr *) &caddr, &clen);
		if (0 > In)
			fatal("could not accept connection for network input: %s\n",strerror(errno));
	} while (memcmp(&caddr.sin_addr,h->h_addr_list[0],h->h_length));
}



void openNetworkOutput(const char *host, unsigned short port)
{
	struct sockaddr_in saddr;
	struct hostent *h = 0;

	debugmsg("openNetworkOutput(%s,%hu)\n",host,port);
	infomsg("creating socket for network output...\n");
	Out = socket(AF_INET, SOCK_STREAM, 6);
	if (0 > Out)
		fatal("could not create socket for network output: %s\n",strerror(errno));
	bzero((void *) &saddr, sizeof(saddr));
	saddr.sin_port = htons(port);
	infomsg("resolving server host...\n");
	if (0 == (h = gethostbyname(host)))
		fatal("could not resolve server hostname: %s\n",hstrerror(h_errno));
	saddr.sin_family = h->h_addrtype;
	memcpy(&saddr.sin_addr,h->h_addr_list[0],h->h_length);
	infomsg("connecting to server (%x)...\n",saddr.sin_addr);
	if (0 > connect(Out, (struct sockaddr *) &saddr, sizeof(saddr)))
		fatal("could not connect to server: %s\n",strerror(errno));
}



void getNetVars(const char **argv, int *c, const char **server, unsigned short *port)
{
	char *tmpserv;
	
	tmpserv = malloc(strlen(argv[*c] + 1));
	if (0 == tmpserv)
		fatal("out of memory\n");
	if (1 < sscanf(argv[*c],"%[0-9a-zA-Z.]:%hu",tmpserv,port)) {
		*server = tmpserv;
		return;
	}
	free((void *) tmpserv);
	if (0 != (*port = atoi(argv[*c])))
		return;
	*server = argv[*c];
	*port = atoi(argv[*c]);
	if (*port)
		(*c)++;
}



void version(void)
{
	fprintf(stderr,
		"mbuffer version "VERSION"\n"\
		"Copyright 2001-2003 - T. Maier-Komor\n"\
		"License: GPL2 - see file COPYING\n");
	exit(0);
}



void usage(void)
{
	fprintf(stderr,
		"usage: mbuffer [Options]\n"
		"Options:\n"
		"-b <num>   : use <num> blocks for buffer (default %llu)\n"
		"-s <size>  : use block of <size> bytes for buffer (default %llu)\n"
		"-m <size>  : use buffer of a total of <size> bytes\n"
		"-t         : use memory mapped temporary file (for huge buffer)\n"
		"-d         : use blocksize of device for output\n"
		"-P <num>   : start writing after buffer has been filled more than <num>%%\n"
		"-p <num>   : start reading after buffer has been filled less than <num>%%\n"
		"-i <file>  : use <file> for input\n"
		"-o <file>  : use <file> for output\n"
		"-I <h>:<p> : use network port <port> as input\n"
		"-O <h>:<p> : output data to host <h> and port <p>\n"
		"-n <num>   : <num> volumes for input\n"
		"-T <file>  : as -t but uses <file> as buffer\n"
		"-l <file>  : use <file> for logging messages\n"
		"-u <num>   : pause <num> milliseconds after each write\n"
		"-f         : overwrite existing files\n"
		"-a <time>  : autoloader which needs <time> seconds to reload\n"
		"-v <level> : set verbose level to <level> (valid values are 0..5)\n"
		"-q         : quiet - do not display the status on stderr\n"
		"--md5      : generate md5 hash of transfered data\n"
		"--version  : print version information\n"
		"Unsupported buffer options: -t -Z -B\n",
		Numblocks,Blocksize);
	exit(0);
}



unsigned long long calcint(const char **argv, int c, unsigned long long d)
{
	char ch;
	unsigned long long i;
	
	switch (sscanf(argv[c],"%llu%c",&i,&ch)) {
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
	int c, err, optMset = 0, optSset = 0, optBset = 0;
#ifdef HAVE_ST_BLKSIZE
	struct stat st;
	int setOutsize = 0;
#endif
	unsigned short netPortIn = 0;
	const char *server = 0, *client = 0;
	unsigned short netPortOut = 0;
	
	Log = stderr;
	for (c = 1; c < argc; c++) {
		if (!argcheck("-s",argv,&c)) {
			Blocksize = Outsize = calcint(argv,c,Blocksize);
			optSset = 1;
			debugmsg("Blocksize = %llu\n",Blocksize);
		} else if (!argcheck("-m",argv,&c)) {
			totalmem = calcint(argv,c,totalmem);
			optMset = 1;
			debugmsg("totalmem = %llu\n",totalmem);
		} else if (!argcheck("-b",argv,&c)) {
			Numblocks = (atoi(argv[c])) ? (atoi(argv[c])) : Numblocks;
			optBset = 1;
			debugmsg("Numblocks = %llu\n",Numblocks);
#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
		} else if (!argcheck("-d",argv,&c)) {
			setOutsize = 1;
			debugmsg("setting output size according to the blocksize of the device\n");
#else
			fatal("cannot determine blocksize of device (unsupported by OS)\n");
#endif
		} else if (!argcheck("-v",argv,&c)) {
			Verbose = (atoi(argv[c])) ? (atoi(argv[c])) : Verbose;
			debugmsg("Verbose = %d\n",Verbose);
		} else if (!argcheck("-u",argv,&c)) {
			Pause = (atoi(argv[c])) ? (atoi(argv[c])) * 1000 : Pause;
			debugmsg("Pause = %d\n",Pause);
		} else if (!argcheck("-n",argv,&c)) {
			Multivolume = atoi(argv[c]) - 1;
			if (Multivolume <= 0)
				fatal("argument for number of volumes must be > 0\n");
			debugmsg("Multivolume = %d\n",Multivolume);
		} else if (!argcheck("-i",argv,&c)) {
			Infile = argv[c];
			debugmsg("Infile = %s\n",Infile);
		} else if (!argcheck("-o",argv,&c)) {
			Outfile = argv[c];
			debugmsg("Outfile = \"%s\"\n",Outfile);
		} else if (!argcheck("-I",argv,&c)) {
			getNetVars(argv,&c,&client,&netPortIn);
			debugmsg("Network input set to %s:%hu\n",client,netPortIn);
		} else if (!argcheck("-O",argv,&c)) {
			getNetVars(argv,&c,&server,&netPortOut);
			debugmsg("Output: server = %s, port = %hu\n",server,netPortOut);
		} else if (!argcheck("-T",argv,&c)) {
			Tmpfile = malloc(strlen(argv[c]) + 1);
			if (!Tmpfile)
				fatal("out of memory\n");
			strcpy(Tmpfile, argv[c]);
			Memmap = 1;
			debugmsg("Tmpfile = %s\n",Tmpfile);
		} else if (!argcheck("-l",argv,&c)) {
			Log = fopen(argv[c],"w");
			if (0 == Log) {
				Log = stderr;
				errormsg("error opening log file: %s\n",strerror(errno));
			}
			debugmsg("logFile set to %s\n",argv[c]);
		} else if (!strcmp("-t",argv[c])) {
			Memmap = 1;
			debugmsg("mm = 1\n");
		} else if (!strcmp("-f",argv[c])) {
			Nooverwrite = 0;
			debugmsg("Nooverwrite = 0\n");
		} else if (!strcmp("-q",argv[c])) {
			debugmsg("disabling display of status\n");
			Status = 0;
		} else if (!argcheck("-a",argv,&c)) {
			Autoloader = atoi(argv[c]);
			debugmsg("Autoloader time = %d\n",Autoloader);
		} else if (!argcheck("-P",argv,&c)) {
			if (1 != sscanf(argv[c],"%f",&StartWrite))
				StartWrite = 0;
			StartWrite /= 100;
			if ((StartWrite > 1) || (StartWrite <= 0))
				fatal("error in argument -P: must be bigger than 0 and less or equal 100");
			debugmsg("StartWrite = %1.2f\n",StartWrite);
		} else if (!argcheck("-p",argv,&c)) {
			if (1 == sscanf(argv[c],"%f",&StartRead))
				StartRead /= 100;
			else
				StartRead = 1.0;
			if ((StartRead >= 1) || (StartRead < 0))
				fatal("error in argument -p: must be bigger or equal to 0 and less than 100");
			debugmsg("StartRead = %1.2f\n",StartRead);
		} else if (!strcmp("--help",argv[c])) {
			usage();
		} else if (!strcmp("-h",argv[c])) {
			usage();
		} else if (!strcmp("--version",argv[c])) {
			version();
		} else if (!strcmp("--md5",argv[c])) {
#ifdef MHASH
			Hash = 1;
			MD5hash = mhash_init(MHASH_MD5);
			if (MHASH_FAILED == MD5hash) {
				errormsg("error initializing md5 hasing - will not generate hash...");
				Hash = 0;
			}
#elif defined MD5
			Hash = 1;
			MD5Init(&md5ctxt);
#else
			warningmsg("md5 hash support has not been compiled in!");
#endif
		} else
			fatal("unknown option \"%s\"\n",argv[c]);
	}

	/* consistency check for options */
	if (optBset&optSset&optMset) {
		if (Numblocks * Blocksize != totalmem)
			fatal("inconsistent options: blocksize * number of blocks != totalsize!\n");
	} else if ((!optBset&optSset&optMset) || (optMset&!optBset&!optSset)) {
		Numblocks = totalmem / Blocksize;
		infomsg("Numblocks = %llu, Blocksize = %llu, totalmem = %llu\n",Numblocks,Blocksize,totalmem);
	} else if (optBset&!optSset&optMset) {
		Blocksize = totalmem / Numblocks;
		infomsg("blocksize = %llu\n",Blocksize);
	}
	if ((StartRead < 1) && (StartWrite > 0))
		fatal("setting both low watermark and high watermark doesn't make any sense...");
	if (Autoloader) {
		if ((!Outfile) && (!Infile))
			fatal("Setting autoloader time without using a device doesn't make any sense!\n");
		if (Outfile && Infile) {
			fatal("Which one is your autoloader? Input or output? Replace input or output with a pipe.\n");
		}
	}
	if (Infile && netPortIn)
		fatal("Setting both network input port and input file doesn't make sense!\n");
	if (Outfile && netPortOut)
		fatal("Setting both network output and output file doesn't make sense!\n");
	if ((0 != client) && (0 == netPortIn))
		fatal("You need to set a network port for network input!\n");
	if ((0 == netPortOut) ^ (0 == server))
		fatal("When sending data to a server, both servername and port must be set!\n");

	/* multi volume input consistency checking */
	if ((Multivolume) && (!Infile))
		fatal("multi volume support for input needs an explicit given input device (option -i)\n");

	/* create buffer */
	Buffer = (char **) malloc(Numblocks * sizeof(char *));
	if (!Buffer)
		fatal("Could not allocate enough memory!\n");
	if (Memmap) {
		infomsg("mapping temporary file to memory with %llu blocks with %llu byte (%llu kB total)...\n",Numblocks,Blocksize,(Numblocks*Blocksize) >> 10);
		if (!Tmpfile) {
			Tmpfile = malloc(20 * sizeof(char));
			if (!Tmpfile)
				fatal("out of memory\n");
			strcpy(Tmpfile,"/tmp/mbuffer-XXXXXX");
			Tmp = mkstemp(Tmpfile);
		} else {
			Tmp = open(Tmpfile,O_RDWR|O_CREAT|O_EXCL);
		}
		if (-1 == Tmp)
			fatal("could not create temporary file (%s): %s\n",Tmpfile,strerror(errno));
		/* resize the file. Needed - at least under linux, who knows why? */
		if (-1 == lseek(Tmp,Numblocks * Blocksize - sizeof(int),SEEK_SET))
			fatal("could not resize temporary file: %s\n",strerror(errno));
		if (-1 == write(Tmp,&c,sizeof(int)))
			fatal("could not resize temporary file: %s\n",strerror(errno));
		Buffer[0] = mmap(0,Blocksize*Numblocks,PROT_READ|PROT_WRITE,MAP_PRIVATE,Tmp,0);
		if (MAP_FAILED == Buffer[0])
			fatal("could not map buffer-file to memory: %s\n",strerror(errno));
		debugmsg("temporary file mapped to address %p\n",Buffer[0]);
	} else {
		infomsg("allocating memory for %llu blocks with %llu byte (%llu kB total)...\n",Numblocks,Blocksize,(Numblocks*Blocksize) >> 10);
		Buffer[0] = (char *) malloc(Blocksize * Numblocks);
	}
	for (c = 1; c < Numblocks; c++)
		Buffer[c] = Buffer[0] + Blocksize * c;

	debugmsg("creating semaphores...\n");
	if (0 != sem_init(&Buf2Dev,0,0))
		fatal("Error creating semaphore Buf2Dev: %s\n",strerror(errno));
	if (0 != sem_init(&Dev2Buf,0,Numblocks))
		fatal("Error creating semaphore Dev2Buf: %s\n",strerror(errno));
	if (0 != sem_init(&PercentageHigh,0,0))
		fatal("Error creating semaphore PercentageHigh: %s\n",strerror(errno));
	if (0 != sem_init(&PercentageLow,0,0))
		fatal("Error creating semaphore PercentageLow: %s\n",strerror(errno));
	if (0 != pthread_mutex_init(&TermMut,0))
		fatal("Error creating mutex for Terminal: %s\n",strerror(errno));

	debugmsg("opening streams...\n");
	if (Infile) {
		if (-1 == (In = open(Infile,O_RDONLY)))
			fatal("could not open input file: %s\n",strerror(errno));
	} else if (netPortIn) {
		openNetworkInput(client,netPortIn);
	} else
		In = fileno(stdin);
	if (Outfile) {
		if (-1 == (Out = open(Outfile,Nooverwrite|O_CREAT|O_WRONLY|O_TRUNC|O_SYNC,0666)))
			fatal("could not open output file: %s\n",strerror(errno));
	} else if (netPortOut) {
		openNetworkOutput(server,netPortOut);
	} else
		Out = fileno(stdout);

#ifdef HAVE_ST_BLKSIZE
	debugmsg("checking output device...\n");
	if (-1 == fstat(Out,&st))
		fatal("could not stat output: %s\n",strerror(errno));
	if (S_ISBLK(st.st_mode) || S_ISCHR(st.st_mode)) {
		infomsg("blocksize is %d bytes on output device\n",st.st_blksize);
		if ((Blocksize < st.st_blksize) || (Blocksize % st.st_blksize != 0)) {
			warningmsg("Blocksize should be a multiple of the blocksize of the output device!\n"
				"This can cause problems with some device/OS combinations...\n"
				"Blocksize on output device is %d (default for tar is 10k)\n",st.st_blksize);
			if (setOutsize)
				fatal("unable to set output blocksize\n");
		} else {
			if (setOutsize) {
				infomsg("setting output blocksize to %d\n",st.st_blksize);
				Outsize = st.st_blksize;
			}
		}
	} else
		infomsg("no device on output stream\n");
#else
	warningmsg("Could not stat output device (unsupported by system)!\n"
		   "This can result in incorrect written data when\n"
		   "using multiple volumes. Continue at your own risk!\n");
#endif

	debugmsg("accessing terminal...\n");
	Terminal = fopen("/dev/tty","r+");
	if (!Terminal) {
		errormsg("could not open terminal: %s\n",strerror(errno));
		warningmsg("multi volume support turned off");
		Status = 0;
	}

	debugmsg("registering signals...\n");
	if (SIG_ERR == signal(SIGINT,sigHandler))
		warningmsg("error registering new SIGINT handler: %s\n",strerror(errno));
	if (SIG_ERR == signal(SIGTERM,sigHandler))
		warningmsg("error registering new SIGINT handler: %s\n",strerror(errno));

	debugmsg("starting threads...\n");
	err = pthread_create(&Writer,0,&outputThread,0);
	assert(0 == err);
	err = pthread_create(&Reader,0,&inputThread,0);
	assert(0 == err);
	if (Status)
		statusThread();
	else {
		pthread_join(Reader,0);
		pthread_join(Writer,0);
	}
	return 0;
}

// vim:tw=0
