/*
 * This file is licensed according to the GPLv2. See file LICENSE for details.
 * author: Thomas Maier-Komor
 */

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
#include <stropts.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <termios.h>
#include <unistd.h>

#ifdef HAVE_SENDFILE
#include <sys/sendfile.h>
/* if this sendfile implementation does not support sending from buffers,
   disable sendfile support */
	#ifndef SFV_FD_SELF
	#ifdef __GNUC__
	#warning Your sendfile implementation does not seem to support sending from buffers - disabling sendfile support.
	#endif
	#undef HAVE_SENDFILE
	#endif
#endif

#ifdef HAVE_LIBMHASH
#include <mhash.h>
#elif defined HAVE_LIBMD5
#include <md5.h>
#elif defined HAVE_LIBSSL
#include <openssl/md5.h>
#endif

#ifndef _POSIX_THREADS
#error posix threads are required
#endif

#ifndef _POSIX_SEMAPHORES
#error posix sempahores are required
#endif

/* headers needed for networking */
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>


#ifdef O_LARGEFILE
#define LARGEFILE O_LARGEFILE
#else
#define LARGEFILE 0
#endif

static pthread_t Reader, Writer;
static long Verbose = 3, In = 0, Out = 0, Tmp = 0, Pause = 0, Memmap = 0,
	Status = 1, Nooverwrite = O_EXCL, Outblocksize = 0,
	Autoloader = 0, Autoload_time = 0, Sock = 0, OptSync = 0,
	ErrorOccured = 0;
static unsigned long Outsize = 10240;
#if defined HAVE_LIBSSL || defined HAVE_LIBMD5 || defined HAVE_LIBMHASH
static long  Hash = 0;
#endif
static volatile int Finish = 0, Terminate = 0;
static unsigned long long  Rest = 0, Blocksize = 10240, Numblocks = 256,
	MaxReadSpeed = 0, MaxWriteSpeed = 0;
static volatile unsigned long long Numin = 0, Numout = 0;
static double StartWrite = 0, StartRead = 1;
static char *Tmpfile = 0, **Buffer;
static const char *Infile = 0, *Outfile = 0, *Autoload_cmd = 0;
static int Multivolume = 0, Memlock = 0, Sendout = 0;
#ifdef HAVE_LIBMHASH
static MHASH MD5hash;
#elif defined HAVE_LIBMD5
static MD5_CTX md5ctxt;
#elif defined HAVE_LIBSSL
static MD5_CTX md5ctxt;
#endif
static sem_t Dev2Buf, Buf2Dev, PercentageLow, PercentageHigh;
static pthread_mutex_t TermMut = PTHREAD_MUTEX_INITIALIZER;
static FILE *Log = 0, *Terminal = 0;
static struct timeval Starttime;


#ifdef DEBUG
static void debugmsg(const char *msg, ...)
{
	if (Verbose >= 5) {
		va_list val;
		va_start(val,msg);
		(void) vfprintf(Log,msg,val);
		va_end(val);
	}
}
#else
#define debugmsg(...)
#endif

static void infomsg(const char *msg, ...)
{
	if (Verbose >= 4) {
		va_list val;
		va_start(val,msg);
		(void) vfprintf(Log,msg,val);
		va_end(val);
	}
}

static void warningmsg(const char *msg, ...)
{
	if (Verbose >= 3) {
		va_list val;
		va_start(val,msg);
		(void) fprintf(Log,"warning: ");
		(void) vfprintf(Log,msg,val);
		va_end(val);
	}
}

static void errormsg(const char *msg, ...)
{
	ErrorOccured = 1;
	if (Verbose >= 2) {
		va_list val;
		va_start(val,msg);
		(void) fprintf(Log,"error: ");
		(void) vfprintf(Log,msg,val);
		va_end(val);
	}
}


static void fatal(const char *msg, ...)
{
	if (Verbose >= 1) {
		va_list val;
		va_start(val,msg);
		(void) fprintf(Log,"fatal: ");
		(void) vfprintf(Log,msg,val);
		va_end(val);
	}
	exit(EXIT_FAILURE);
}



static void summary(unsigned long long numb, double secs)
{
	int h = (int) secs/3600, m = (int) (secs - h * 3600)/60;
	double av = (double)numb/secs;
	
	secs -= m * 60 + h * 3600;
	(void) fprintf(Terminal,"\nsummary: ");
	if (numb < 1331ULL)			/* 1.3 kB */
		(void) fprintf(Terminal,"%llu Byte in ",numb);
	else if (numb < 1363149ULL)		/* 1.3 MB */
		(void) fprintf(Terminal,"%.1f kB in ",(double)numb / 1024);
	else if (numb < 1395864371ULL)		/* 1.3 GB */
		(void) fprintf(Terminal,"%.1f MB in ",(double)numb / (double)(1<<20));
	else
		(void) fprintf(Terminal,"%.1f GB in ",(double)numb / (1<<30));
	if (h)
		(void) fprintf(Terminal,"%d h ",h);
	if (m)
		(void) fprintf(Terminal,"%02d min ",m);
	(void) fprintf(Terminal,"%02.1f sec - ",secs);
	(void) fprintf(Terminal,"average of ");
	if (av < 1331)
		(void) fprintf(Terminal,"%.0f B/s\n",av);
	else if (av < 1363149)			/* 1.3 MB */
		(void) fprintf(Terminal,"%.1f kB/s\n",av/1024);
	else if (av < 1395864371)		/* 1.3 GB */
		(void) fprintf(Terminal,"%.1f MB/s\n",av/1048576);
	else
		(void) fprintf(Terminal,"%.1f GB/s\n",av/1073741824);	/* OK - this is really silly - at least now in 2003, yeah and still in 2005... */
#ifdef HAVE_LIBMHASH
	if (Hash) {
		unsigned char hashvalue[16];
		int i;
		
		mhash_deinit(MD5hash,hashvalue);
		(void) fprintf(Terminal,"md5 hash:");
		for (i = 0; i < 16; ++i)
			(void) fprintf(Terminal," %02x",hashvalue[i]);
		(void) fprintf(Terminal,"\n");
	}
#elif defined HAVE_LIBMD5
	if (Hash) {
		unsigned char hashvalue[16];
		int i;
		
		MD5Final(hashvalue,&md5ctxt);
		(void) fprintf(Terminal,"md5 hash:");
		for (i = 0; i < 16; ++i)
			(void) fprintf(Terminal," %02x",hashvalue[i]);
		(void) fprintf(Terminal,"\n");
	}
#elif defined HAVE_LIBSSL
	if (Hash) {
		unsigned char hashvalue[16];
		int i;
		
		MD5_Final(hashvalue,&md5ctxt);
		(void) fprintf(Terminal,"md5 hash:");
		for (i = 0; i < 16; ++i)
			(void) fprintf(Terminal," %02x",hashvalue[i]);
		(void) fprintf(Terminal,"\n");
	}
#endif
}



static RETSIGTYPE sigHandler(int signr)
{
	switch (signr) {
	case SIGINT:
	case SIGTERM:
		Terminate = 1;
		break;
	default:
		(void) raise(SIGABRT);
	}
}



static void statusThread(void) 
{
	struct timeval last, now;
	double in = 0, out = 0, diff, fill;
	unsigned long long total, lin = 0, lout = 0;
	int unwritten = 1;	/* assumption: initially there is at least one unwritten block */
	int ret;

	(void) gettimeofday(&Starttime,0);
	last = Starttime;
#ifdef __alpha
	(void) usleep(1000);	/* needed on alpha (stderr fails with fpe on nan) */
#endif
	while (!(Finish && (unwritten == 0)) && (Terminate == 0)) {
		int err;
		char id = 'k', od = 'k', td = 'k';
		err = sem_getvalue(&Buf2Dev,&unwritten);
		assert(0 == err);
		fill = (double)unwritten / (double)Numblocks * 100.0;
		(void) gettimeofday(&now,0);
		diff = now.tv_sec - last.tv_sec + (double) now.tv_usec / 1000000 - (double) last.tv_usec / 1000000;
		in = ((Numin - lin) * Blocksize) >> 10;
		in /= diff;
		if (in > 3 * 1024) {
			id = 'M';
			in /= 1024;
		}
		out = ((Numout - lout) * Blocksize) >> 10;
		out /= diff;
		if (out > 3 * 1024) {
			od = 'M';
			out /= 1024;
		}
		lin = Numin;
		lout = Numout;
		last = now;
		total = (Numout * Blocksize) >> 10;
		if (total > 3 * 1024) {
			td = 'M';
			total >>= 10;
		}
		if (total > 3 * 1024) {
			td = 'G';
			total >>= 10;
		}
		fill = (fill < 0) ? 0 : fill;
		err = pthread_mutex_lock(&TermMut);
		assert(0 == err);
		(void) fprintf(Terminal,"\rin @ %6.1f %cB/s, out @ %6.1f %cB/s, %4llu %cB total, buffer %3.0f%% full",in,id,out,od,total,td,fill);
		(void) fflush(Terminal);
		err = pthread_mutex_unlock(&TermMut);
		assert(0 == err);
		(void) usleep(500000);
	}
	if (Terminate) {
		infomsg("\rterminating...\n");
		(void) pthread_cancel(Writer);
		(void) pthread_cancel(Reader);
	}
	(void) fprintf(Terminal,"\n");
	infomsg("statusThread: joining to terminate...\n");
	ret = pthread_join(Reader,0);
	assert(0 == ret);
	ret = pthread_join(Writer,0);
	assert(0 == ret);
	if (Memmap) {
		int ret = munmap(Buffer[0],Blocksize*Numblocks);
		assert(ret == 0);
	}
	(void) close(Tmp);
	(void) gettimeofday(&now,0);
	diff = now.tv_sec - Starttime.tv_sec + (double) now.tv_usec / 1000000 - (double) Starttime.tv_usec / 1000000;
	summary(Numout * Blocksize + Rest, diff);
	if (ErrorOccured)
		exit(EXIT_FAILURE);
	exit(EXIT_SUCCESS);
}


static long long timediff(struct timeval *t1, struct timeval *t2)
{
	long long tdiff;
	tdiff = (t1->tv_sec - t2->tv_sec) * 1000000;
	tdiff += t1->tv_usec - t2->tv_usec;
	return tdiff;
}


static unsigned long long enforceSpeedLimit(unsigned long long limit, unsigned long long num, struct timeval *last)
{
	static long long minwtime = 0;
	struct timeval now;
	long long tdiff;
	double dt;
	
	if (minwtime == 0)
		minwtime = 1000000 / sysconf(_SC_CLK_TCK);
	(void) gettimeofday(&now,0);
	tdiff = timediff(&now,last);
	if (tdiff < 0)
		return num;
	if (tdiff > minwtime*5) {
		(void) gettimeofday(last,0);
		return num;
	}
	dt = (double)tdiff / 1E6;
	num += Blocksize;
	if ((num/dt) > (double)limit) {
		double req = (double)num/limit - dt;
		long long w = (long long) (req * 1E6);
		/*
		 * The following threshold is a heuristic value.
		 * By experimenting, I found out that using the minimum wait time
		 * causes a jitter that cannot be adjusted cleanly and results in
		 * a higher transfer rate than requested for transfers of some MB/s.
		 * With a limit of 20-30MB/s this issue disappears.
		 * 
		 * By using a threshold of 8*minwtime the speed limit is held much 
		 * better for 2 MB/s. For very low limits i.e. < 200k/s this high 
		 * threshold is contraproductive. If you really want to use so low
		 * limits, please change "8 * minwtime" to "minwtime".
		 *
		 * Feel free to send me feedback concerning this threshold or an
		 * alternative algorithm that gets it right for any value.
		 */
		if (w > 8 * minwtime) {
			long long slept, stime;
			stime = w / minwtime;
			stime *= minwtime;
			debugmsg("enforceSpeedLimit(): sleeping for %lld usec\n",stime);
			(void) usleep(stime);
			(void) gettimeofday(last,0);
			slept = timediff(last,&now);
			debugmsg("enforceSpeedLimit(): slept for %lld usec\n",slept);
			slept -= stime;
			last->tv_usec -= slept / 1000000;
			last->tv_usec -= (slept % 1000000);
			if (last->tv_usec < 0) {
				--last->tv_sec;
				last->tv_usec += 1000000;
			}
			return 0;
		} /* else {
			Sleeping now would cause a slowdown. So we defer this
			sleep until the next block has been transferred. Like
			this we can stay as close to the speed limit as possible.
			}
		*/
	}
	return num;
}


static void requestInputVolume(void)
{
	char cmd_buf[15+strlen(Infile)];
	const char *cmd;
	int err;

	debugmsg("requesting new volume for input\n");
	if (-1 == close(In))
		errormsg("error closing input: %s\n",strerror(errno));
	do {
		if ((Autoloader) && (Infile)) {
			infomsg("requesting change of volume...\n");
			if (Autoload_cmd) {
				cmd = Autoload_cmd;
			} else {
				(void) snprintf(cmd_buf, sizeof(cmd_buf), "mt -f %s offline", Infile);
				cmd = cmd_buf;
			}
			if (-1 == system(cmd)) {
				warningmsg("error running \"%s\" to change volume in autoloader...\n", cmd);
				Autoloader = 0;
				continue;
			}
			infomsg("waiting for drive to get ready...\n");
			if (Autoload_time)
				(void) sleep(Autoload_time);
		} else {
			int err1;
			err1 = pthread_mutex_lock(&TermMut);
			assert(0 == err1);
			(void) fprintf(Terminal,"\ninsert next volume and press return to continue...");
			(void) fflush(Terminal);
			(void) tcflush(fileno(Terminal),TCIFLUSH);
			(void) fgetc(Terminal);
			err1 = pthread_mutex_unlock(&TermMut);
			assert(0 == err1);
		}
		if (-1 == (In = open(Infile,O_RDONLY|LARGEFILE)))
			errormsg("could not reopen input: %s\n",strerror(errno));
	} while (In == -1);
	Multivolume--;
	err = pthread_mutex_lock(&TermMut);
	assert(0 == err);
	(void) fprintf(Terminal,"\nOK - continuing...");
	(void) fflush(Terminal);
	err = pthread_mutex_unlock(&TermMut);
	assert(0 == err);
}



static void *inputThread(void *ignored)
{
	int err;
	int fill = 0;
	unsigned long long num, at = 0, xfer = 0;
	const double startread = StartRead, startwrite = StartWrite;
	struct timeval last;

	(void) gettimeofday(&last,0);
	assert(ignored == 0);
	infomsg("inputThread: starting...\n");
	for (;;) {
		if ((startread < 1) && (fill == Numblocks - 1)) {
			debugmsg("inputThread: buffer full, waiting for it to drain.\n");
			sem_wait(&PercentageLow);
		}
		debugmsg("inputThread: sem_wait\n");
		sem_wait(&Dev2Buf); /* Wait for one or more buffer blocks to be free */
		if (Terminate) {	/* for async termination requests */
			
			debugmsg("inputThread: terminating upon termination request...\n");
			if (-1 == close(In))
				errormsg("error closing input: %s\n",strerror(errno));
			pthread_exit(0);
			return 0;	/* just to make lint happy... */
		}
		num = 0;
		do {
			err = read(In,Buffer[at] + num,Blocksize - num);
			debugmsg("inputThread: read(In, Buffer[%llu] + %llu, %llu) = %d\n", at, num, Blocksize - num, err);
			if ((!err) && (Terminal) && (Multivolume)) {
				requestInputVolume();
			} else if (-1 == err) {
				errormsg("inputThread: error reading at offset %llx: %s\n",Numin*Blocksize,strerror(errno));
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
				#ifdef HAVE_LIBMHASH
				if (Hash)
					mhash(MD5hash,Buffer[at],num);
				#elif defined HAVE_LIBMD5
				if (Hash)
					MD5Update(&md5ctxt,(unsigned char *)Buffer[at],num);
				#elif defined HAVE_LIBSSL
				if (Hash)
					MD5_Update(&md5ctxt,Buffer[at],num);
				#endif
				sem_post(&Buf2Dev);
				sem_post(&PercentageHigh);
				infomsg("inputThread: exiting...\n");
				pthread_exit(0);
			} 
			num += err;
		} while (num < Blocksize);
		debugmsg("inputThread: sem_post\n");
		#ifdef HAVE_LIBMHASH
		if (Hash)
			mhash(MD5hash,Buffer[at],Blocksize);
		#elif defined HAVE_LIBMD5
		if (Hash)
			MD5Update(&md5ctxt,(unsigned char *)Buffer[at],num);
		#elif defined HAVE_LIBSSL
		if (Hash)
			MD5_Update(&md5ctxt,Buffer[at],num);
		#endif
		sem_post(&Buf2Dev);
		if (MaxReadSpeed)
			xfer = enforceSpeedLimit(MaxReadSpeed,xfer,&last);
		sem_getvalue(&Buf2Dev,&fill);
		if (((double) fill / (double) Numblocks) >= startwrite) {
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



static void requestOutputVolume(void)
{
	if (!Outfile) {
		errormsg("End of volume, but not end of input:\n"
			"Output file must be given (option -o) for multi volume support!\n");
		Finish = 1;
		pthread_exit((void *) -1);
	}
	debugmsg("requesting new output volume\n");
	if (-1 == close(Out))
		errormsg("error closing output: %s\n",strerror(errno));
	do {
		if ((Autoloader) && (Outfile)) {
			const char default_cmd[] = "mt -f %s offline";
			char cmd_buf[sizeof(default_cmd)+strlen(Outfile)];
			const char *cmd = Autoload_cmd;

			infomsg("requesting change of volume...\n");
			if (cmd == 0) {
				(void) snprintf(cmd_buf, sizeof(cmd_buf), default_cmd, Infile);
				cmd = cmd_buf;
			}
			if (-1 == system(cmd)) {
				warningmsg("error running \"%s\" to change volume in autoloader...\n", cmd);
				Autoloader = 0;
				continue;
			}
			infomsg("waiting for drive to get ready...\n");
			if (Autoload_time)
				(void) sleep(Autoload_time);
		} else {
			int err;
			err = pthread_mutex_lock(&TermMut);
			assert(0 == err);
			if (Terminal == 0) {
				errormsg("End of volume, but not end of input.\n"
					"Specify an autoload command, if you are working without terminal.\n");
				Finish = 1;
				pthread_exit((void *) -1);
			}
			(void) fprintf(Terminal,"\nvolume full - insert new media and press return whe ready...\n");
			(void) tcflush(fileno(Terminal),TCIFLUSH);
			(void) fgetc(Terminal);
			(void) fprintf(Terminal,"\nOK - continuing...\n");
			err = pthread_mutex_unlock(&TermMut);
			assert(0 == err);
		}
		if (-1 == (Out = open(Outfile,Nooverwrite|O_CREAT|O_WRONLY|O_TRUNC|OptSync|LARGEFILE,0666)))
			errormsg("error reopening output file: %s\n",strerror(errno));
#ifdef __sun
		else if (-1 == directio(Out,DIRECTIO_ON))
			debugmsg("direct I/O hinting failed: %s\n",strerror(errno));
#endif
	} while (-1 == Out);
	infomsg("continuing with next volume\n");
}



static void checkIncompleteOutput(void)
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



static void *outputThread(void *ignored)
{
	unsigned at = 0;
	int fill;
	const double startwrite = StartWrite, startread = StartRead;
	unsigned long long blocksize = Blocksize, num = 0;
	struct timeval last;

	/* initialize last to 0, because we don't want to wait initially */
	(void) gettimeofday(&last,0);
	assert(ignored == 0);
	infomsg("\noutputThread: starting...\n");
	for (;;) {
		unsigned long long rest = blocksize;
		debugmsg("outputThread: sem_wait\n");
		sem_getvalue(&Buf2Dev,&fill);
		if ((fill == 0) && (startwrite > 0)) {
			debugmsg("outputThread: buffer empty, waiting for it to fill\n");
			sem_wait(&PercentageHigh);
		}
		sem_wait(&Buf2Dev);
		if (Terminate) {
			debugmsg("outputThread: terminating upon termination request...\n");
			if (-1 == close(Out)) 
				errormsg("error closing output: %s\n",strerror(errno));;
			pthread_exit(0);
		}
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
			int err;
#ifdef HAVE_SENDFILE
			if (Sendout) {
				off_t baddr = (off_t) Buffer[at] + blocksize - rest;
				err = sendfile(Out,SFV_FD_SELF,&baddr,rest > Outsize ? Outsize : rest);
#ifdef DEBUG
				debugmsg("outputThread: sendfile(Out, SFV_FD_SELF, Buffer[%d] + %llu, %llu) = %d\n", at, blocksize - rest, rest > Outsize ? Outsize : rest, err);
#endif
			} else
#endif
			{
				err = write(Out,Buffer[at] + blocksize - rest, rest > Outsize ? Outsize : rest);
#ifdef DEBUG
				debugmsg("outputThread: write(Out, Buffer[%d] + %llu, %d) = %d\t(rest = %d)\n", at, blocksize - rest, rest > Outsize ? Outsize : rest, err, rest);
#endif
			}
			if ((-1 == err) && (Terminal) && ((errno == ENOMEM) || (errno == ENOSPC))) {
				/* request a new volume - but first check
				 * wheather we are really at the
				 * end of the device */
				checkIncompleteOutput();
				continue;
			} else if (-1 == err) {
				errormsg("outputThread: error writing at offset %llx: %s\n",Blocksize*Numout+blocksize-rest,strerror(errno));
				Terminate = 1;
				sem_post(&Dev2Buf);
				pthread_exit((void *) -1);
			}
			rest -= err;
			if (MaxWriteSpeed) {
				num = enforceSpeedLimit(MaxWriteSpeed,num,&last);
			}
			if (Pause)
				(void) usleep(Pause);
		} while (rest > 0);
		debugmsg("outputThread: sem_post\n");
		sem_post(&Dev2Buf);
		at++;
		if (Numblocks == at)
			at = 0;
		if (Finish) {
			sem_getvalue(&Buf2Dev,&fill);
			if (0 == fill) {
				infomsg("outputThread: syncing...\n");
				while (0 != fsync(Out)) {
					if (errno == EINTR) {
						continue;
					} else if (errno == EINVAL) {
						warningmsg("output does not support syncing: omitted.");
						break;
					} else {
						fatal("error syncing: %s\n",strerror(errno));
					}
				}
				infomsg("outputThread: finished - exiting...\n");
				if (-1 == close(Out))
					errormsg("error closing output: %s\n",strerror(errno));
				pthread_exit(0);
				return 0;	/* just for lint */
			}
		}
		sem_getvalue(&Buf2Dev,&fill);
		if ((startread < 1) && ((double)fill / (double)Numblocks) < startread) {
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



static void openNetworkInput(const char *host, unsigned short port)
{
	struct sockaddr_in saddr;
	struct sockaddr_in caddr;
	socklen_t clen = sizeof(caddr);
	struct hostent *h = 0, *r = 0;
	const int reuse_addr = 1;

	debugmsg("openNetworkInput(%s,%hu)\n",host,port);
	infomsg("creating socket for network input...\n");
	Sock = socket(AF_INET, SOCK_STREAM, 6);
	if (0 > Sock)
		fatal("could not create socket for network input: %s\n",strerror(errno));
	if (-1 == setsockopt(Sock, SOL_SOCKET, SO_REUSEADDR, &reuse_addr, sizeof(reuse_addr)))
		warningmsg("cannot set socket to reuse address: %s\n",strerror(errno));
	if (host) {
		debugmsg("resolving hostname of input interface...\n");
		if (0 == (h = gethostbyname(host)))
			fatal("could not resolve server hostname: %s\n",hstrerror(h_errno));
	}
	bzero((void *) &saddr, sizeof(saddr));
	saddr.sin_family = AF_INET;
	saddr.sin_addr.s_addr = htonl(INADDR_ANY);
	saddr.sin_port = htons(port);
	infomsg("binding socket...\n");
	if (0 > bind(Sock, (struct sockaddr *) &saddr, sizeof(saddr)))
		fatal("could not bind to socket for network input: %s\n",strerror(errno));
	infomsg("listening on socket...\n");
	if (0 > listen(Sock,1))		/* accept only 1 incoming connection */
		fatal("could not listen on socket for network input: %s\n",strerror(errno));
	for (;;) {
		char **p;
		infomsg("waiting to accept connection...\n");
		In = accept(Sock, (struct sockaddr *)&caddr, &clen);
		if (0 > In)
			fatal("could not accept connection for network input: %s\n",strerror(errno));
		if (host == 0) {
			infomsg("connection accepted");
			return;
		}
		for (p = h->h_addr_list; *p; ++p) {
			if (0 == memcmp(&caddr.sin_addr,*p,h->h_length)) {
				infomsg("connection accepted");
				return;
			}
		}
		r = gethostbyaddr((char *)&caddr.sin_addr,sizeof(caddr.sin_addr.s_addr),AF_INET);
		if (r)
			errormsg("rejected connection from %s (%s)\n",r->h_name,inet_ntoa(caddr.sin_addr));
		else
			errormsg("rejected connection from %s\n",inet_ntoa(caddr.sin_addr));
		if (-1 == close(In))
			errormsg("error closing input: %s\n",strerror(errno));
	}
}



static void openNetworkOutput(const char *host, unsigned short port)
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
	assert(h->h_length <= sizeof(saddr.sin_addr));
	(void) memcpy(&saddr.sin_addr,h->h_addr_list[0],h->h_length);
	infomsg("connecting to server (%x)...\n",saddr.sin_addr);
	if (0 > connect(Out, (struct sockaddr *) &saddr, sizeof(saddr)))
		fatal("could not connect to server: %s\n",strerror(errno));
}



static void getNetVars(const char **argv, int *c, const char **host, unsigned short *port)
{
	char *tmphost;
	
	tmphost = malloc(strlen(argv[*c] + 1));
	if (0 == tmphost)
		fatal("out of memory\n");
	if (1 < sscanf(argv[*c],"%[0-9a-zA-Z.]:%hu",tmphost,port)) {
		*host = tmphost;
		return;
	}
	free((void *) tmphost);
	if (1 == sscanf(argv[*c],":%hu",port)) {
		return;
	}
	if (0 != (*port = atoi(argv[*c])))
		return;
	*host = argv[*c];
	*port = atoi(argv[*c]);
	if (*port)
		(*c)++;
}



static void version(void)
{
	(void) fprintf(stderr,
		"mbuffer version "VERSION"\n"\
		"Copyright 2001-2006 - T. Maier-Komor\n"\
		"License: GPL2 - see file COPYING\n");
	exit(EXIT_SUCCESS);
}



static void usage(void)
{
	(void) fprintf(stderr,
		"usage: mbuffer [Options]\n"
		"Options:\n"
		"-b <num>   : use <num> blocks for buffer (default %llu)\n"
		"-s <size>  : use block of <size> bytes for buffer (default %llu)\n"
		"-m <size>  : use buffer of a total of <size> bytes\n"
#ifdef _POSIX_MEMLOCK
		"-L         : lock buffer in memory (unusable with file based buffers)\n"
#endif
#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
		"-d         : use blocksize of device for output\n"
#endif
		"-P <num>   : start writing after buffer has been filled more than <num>%%\n"
		"-p <num>   : start reading after buffer has been filled less than <num>%%\n"
		"-i <file>  : use <file> for input\n"
		"-o <file>  : use <file> for output\n"
		"-I <h>:<p> : use network port <port> as input, allow only host <h> to connect\n"
		"-I <p>     : use network port <port> as input\n"
		"-O <h>:<p> : output data to host <h> and port <p>\n"
		"-n <num>   : <num> volumes for input\n"
		"-t         : use memory mapped temporary file (for huge buffer)\n"
		"-T <file>  : as -t but uses <file> as buffer\n"
		"-l <file>  : use <file> for logging messages\n"
		"-u <num>   : pause <num> milliseconds after each write\n"
		"-r <rate>  : limit read rate to <rate> B/s, where <rate> can be given in k,M,G\n"
		"-R <rate>  : same as -r for writing; use eiter one, if your tape is too fast\n"
		"-f         : overwrite existing files\n"
		"-a <time>  : autoloader which needs <time> seconds to reload\n"
		"-A <cmd>   : issue command <cmd> to request new volume\n"
		"-v <level> : set verbose level to <level> (valid values are 0..5)\n"
		"-q         : quiet - do not display the status on stderr\n"
		"-c         : write with synchronous data integrity support\n"
#if defined HAVE_LIBSSL || defined HAVE_LIBMD5 || defined HAVE_LIBMHASH
		"-H\n"
		"--md5      : generate md5 hash of transfered data\n"
#endif
		"-V\n"
		"--version  : print version information\n"
		"Unsupported buffer options: -t -Z -B\n",
		Numblocks,Blocksize);
	exit(EXIT_SUCCESS);
}



static unsigned long long calcint(const char **argv, int c, unsigned long long d)
{
	char ch;
	unsigned long long i;
	
	switch (sscanf(argv[c],"%llu%c",&i,&ch)) {
	default:
		assert(0);
		break;
	case 2:
		switch (ch) {
		case 'k':
			i <<= 10;
			return i;
		case 'M':
			i <<= 20;
			return i;
		case 'G':
			i <<= 30;
			return i;
		case '%':
			if (i >= 100)
				fatal("invalid value for percentage (must be < 100)\n");
			return i;
		case 'b':
		case 'B':
			if (i < 128)
				fatal("invalid value for number of bytes");
			return i;
		default:
			errormsg("unrecognized size charakter \"%c\" for option \"%s\"\n",ch,argv[c-1]);
			return d;
		}
	case 1:
		return i;
	case 0:
		break;
	}
	errormsg("unrecognized argument \"%s\" for option \"%s\"\n",argv[c],argv[c-1]);
	return d;
}



static int argcheck(const char *opt, const char **argv, int *c)
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
	unsigned u;
	long mxnrsem;
	int err, optMset = 0, optSset = 0, optBset = 0, c;
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
			if (Blocksize < 100)
				fatal("cannot set blocksize as percentage of total physical memory\n");
		} else if (!argcheck("-m",argv,&c)) {
			totalmem = calcint(argv,c,totalmem);
			optMset = 1;
			if (totalmem < 100) {
				long pgsz, nump;
				pgsz = sysconf(_SC_PAGESIZE);
				assert(pgsz > 0);
				nump = sysconf(_SC_PHYS_PAGES);
				assert(pgsz > 0);
				debugmsg("total # of phys pages: %li (pagesize %li)\n",nump,pgsz);
				totalmem = ((unsigned long long) nump * pgsz * totalmem) / 100 ;
			}
			debugmsg("totalmem = %llu\n",totalmem);
		} else if (!argcheck("-b",argv,&c)) {
			Numblocks = (atoi(argv[c])) ? ((unsigned long long) atoll(argv[c])) : Numblocks;
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
		} else if (!argcheck("-r",argv,&c)) {
			MaxReadSpeed = calcint(argv,c,0);
			debugmsg("MaxReadSpeed = %lld\n",MaxReadSpeed);
		} else if (!argcheck("-R",argv,&c)) {
			MaxWriteSpeed = calcint(argv,c,0);
			debugmsg("MaxWriteSpeed = %lld\n",MaxWriteSpeed);
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
			Sendout = 1;
		} else if (!argcheck("-T",argv,&c)) {
			Tmpfile = malloc(strlen(argv[c]) + 1);
			if (!Tmpfile)
				fatal("out of memory\n");
			(void) strcpy(Tmpfile, argv[c]);
			Memmap = 1;
			debugmsg("Tmpfile = %s\n",Tmpfile);
			if (Memlock) {
				Memlock = 0;
				warningmsg("cannot lock file based buffers in memory\n");
			}
		} else if (!strcmp("-t",argv[c])) {
			Memmap = 1;
			debugmsg("mm = 1\n");
		} else if (!argcheck("-l",argv,&c)) {
			Log = fopen(argv[c],"w");
			if (0 == Log) {
				Log = stderr;
				errormsg("error opening log file: %s\n",strerror(errno));
			}
			debugmsg("logFile set to %s\n",argv[c]);
		} else if (!strcmp("-f",argv[c])) {
			Nooverwrite = 0;
			debugmsg("Nooverwrite = 0\n");
		} else if (!strcmp("-q",argv[c])) {
			debugmsg("disabling display of status\n");
			Status = 0;
		} else if (!strcmp("-c",argv[c])) {
			debugmsg("enabling full synchronous I/O\n");
			OptSync = O_DSYNC;
		} else if (!argcheck("-a",argv,&c)) {
			Autoloader = 1;
			Autoload_time = atoi(argv[c]);
			debugmsg("Autoloader time = %d\n",Autoload_time);
		} else if (!argcheck("-A",argv,&c)) {
			Autoloader = 1;
			Autoload_cmd = argv[c];
			debugmsg("Autoloader command = \"%s\"\n", Autoload_cmd);
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
#ifdef _POSIX_MEMLOCK
		} else if (!strcmp("-L",argv[c])) {
			if (Tmpfile == 0)
				Memlock = 1;
			else
				warningmsg("cannot lock file based buffers in memory\n");
#endif
		} else if (!strcmp("--help",argv[c]) || !strcmp("-h",argv[c])) {
			usage();
		} else if (!strcmp("--version",argv[c]) || !strcmp("-V",argv[c])) {
			version();
		} else if (!strcmp("--md5",argv[c]) || !strcmp("-H",argv[c])) {
#ifdef HAVE_LIBMHASH
			Hash = 1;
			MD5hash = mhash_init(MHASH_MD5);
			if (MHASH_FAILED == MD5hash) {
				errormsg("error initializing md5 hasing - will not generate hash...");
				Hash = 0;
			}
#elif defined HAVE_LIBMD5
			Hash = 1;
			MD5Init(&md5ctxt);
#elif defined HAVE_LIBSSL
			Hash = 1;
			MD5_Init(&md5ctxt);
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

	/* check that we stay within system limits */
	mxnrsem = sysconf(_SC_SEM_VALUE_MAX);
	if (-1 == mxnrsem) {
		warningmsg("unable to determine maximum value of semaphores\n");
	} else if (Numblocks > (unsigned long long) mxnrsem)
		fatal("cannot allocate more than %d blocks.\nThis is a system dependent limit, depending on the maximum semaphore value.\nPlease choose a bigger block size.\n",mxnrsem);

	if (Blocksize * Numblocks > SSIZE_MAX)
		fatal("Cannot address so much memory (%lld*%lld=%lld>%lld).\n",Blocksize,Numblocks, Blocksize*Numblocks,SSIZE_MAX);
	/* create buffer */
	Buffer = (char **) valloc(Numblocks * sizeof(char *));
	if (!Buffer)
		fatal("Could not allocate enough memory: %s\n",strerror(errno));
	if (Memmap) {
		infomsg("mapping temporary file to memory with %llu blocks with %llu byte (%llu kB total)...\n",Numblocks,Blocksize,(Numblocks*Blocksize) >> 10);
		if (!Tmpfile) {
			char tmplname[] = "mbuffer-XXXXXX";
			char *tmpdir = getenv("TMPDIR") ? getenv("TMPDIR") : "/var/tmp";
			char tfilename[sizeof(tmplname) + strlen(tmpdir) + 1];
			(void) strcpy(tfilename,tmpdir);
			(void) strcat(tfilename,"/");
			(void) strcat(tfilename,tmplname);
			Tmp = mkstemp(tfilename);
			Tmpfile = malloc(strlen(tfilename));
			if (!Tmpfile)
				fatal("out of memory: %s\n",strerror(errno));
			(void) strcpy(Tmpfile,tfilename);
			infomsg("tmpfile is %s\n",Tmpfile);
		} else {
			Tmp = open(Tmpfile,O_RDWR|O_CREAT|O_EXCL);
		}
		if (-1 == Tmp)
			fatal("could not create temporary file (%s): %s\n",Tmpfile,strerror(errno));
		(void) unlink(Tmpfile);
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
		Buffer[0] = (char *) valloc(Blocksize * Numblocks);
		if (Buffer[0] == 0)
			fatal("Could not allocate enough memory: %s\n",strerror(errno));
	}
	for (u = 1; u < Numblocks; u++) {
		Buffer[u] = Buffer[0] + Blocksize * u;
		*Buffer[u] = 0;	/* touch every block before locking */
	}

#ifdef _POSIX_MEMLOCK
	if (Memlock) {
		uid_t uid;
		uid = geteuid();
		if (0 != seteuid(0))
			warningmsg("could not change to uid 0 to lock memory (is mbuffer setuid root?)\n");
		else if ((0 != mlock((char *)Buffer,Numblocks * sizeof(char *))) || (0 != mlock(Buffer[0],Blocksize * Numblocks)))
			warningmsg("could not lock buffer in memory: %s\n",strerror(errno));
		err = seteuid(uid);	/* don't give anyone a chance to attack this program, so giveup uid 0 after locking... */
		assert(err == 0);
	}
#endif

	debugmsg("creating semaphores...\n");
	if (0 != sem_init(&Buf2Dev,0,0))
		fatal("Error creating semaphore Buf2Dev: %s\n",strerror(errno));
	if (0 != sem_init(&Dev2Buf,0,Numblocks))
		fatal("Error creating semaphore Dev2Buf: %s\n",strerror(errno));
	if (0 != sem_init(&PercentageHigh,0,0))
		fatal("Error creating semaphore PercentageHigh: %s\n",strerror(errno));
	if (0 != sem_init(&PercentageLow,0,0))
		fatal("Error creating semaphore PercentageLow: %s\n",strerror(errno));

	debugmsg("opening streams...\n");
	if (Infile) {
		if (-1 == (In = open(Infile,O_RDONLY)))
			fatal("could not open input file: %s\n",strerror(errno));
	} else if (netPortIn) {
		openNetworkInput(client,netPortIn);
	} else
		In = fileno(stdin);
	if (Outfile) {
		if (-1 == (Out = open(Outfile,Nooverwrite|O_CREAT|O_WRONLY|O_TRUNC|OptSync,0666)))
			fatal("could not open output file: %s\n",strerror(errno));
#ifdef __sun
		else if (-1 == directio(Out,DIRECTIO_ON))
			debugmsg("direct I/O hinting failed: %s\n",strerror(errno));
#endif
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
				"Blocksize on output device is %d (transfer block size is %lld)\n", st.st_blksize, Blocksize);
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
	if ((Terminal == 0) && (Status != 0)) {
		warningmsg("could not open terminal: %s\n",strerror(errno));
		warningmsg("disabled manual multivolume support and display of throughput");
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
		(void) pthread_join(Reader,0);
		(void) pthread_join(Writer,0);
	}
	return 0;
}

/* vim:tw=0
 */
