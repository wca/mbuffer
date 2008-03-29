/*
 * This file is licensed according to the GPLv2. See file LICENSE for details.
 * Copyright 2000-2008, Thomas Maier-Komor
 */

#include "config.h"
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <malloc.h>
#include <math.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <termios.h>
#include <unistd.h>

#ifdef HAVE_SENDFILE
#ifdef HAVE_SENDFILE_H
#include <sys/sendfile.h>
#endif
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
#elif defined HAVE_LIBCRYPTO
#include <openssl/md5.h>
#endif

/*
 * _POSIX_THREADS is only defined if full thread support is available.
 * We don't need full thread support, so we skip this test...
 * #ifndef _POSIX_THREADS
 * #error posix threads are required
 * #endif
 */

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

#define MSGSIZE 256

static pthread_t Reader, Writer;
static long Verbose = 3, In = 0, Out = 0, Tmp = 0, Pause = 0, Memmap = 0,
	Status = 1, Nooverwrite = O_EXCL, Outblocksize = 0,
	Autoloader = 0, Autoload_time = 0, Sock = 0, OptSync = 0,
	ErrorOccured = 0;
static unsigned long Outsize = 10240;
#if defined HAVE_LIBCRYPTO || defined HAVE_LIBMD5 || defined HAVE_LIBMHASH
static long  Hash = 0;
#endif
static volatile int
	Finish = 0,	/* this is for graceful termination */
	Terminate = 0;	/* abort execution, because of error or signal */
static unsigned long long  Rest = 0, Blocksize = 10240, Numblocks = 256,
	MaxReadSpeed = 0, MaxWriteSpeed = 0, OutVolsize = 0;
static volatile unsigned long long Numin = 0, Numout = 0;
static volatile int EmptyCount = 0, FullCount = 0;
static double StartWrite = 0, StartRead = 1;
static char *Tmpfile = 0, **Buffer, *Msgbuf, *Msg;
static const char *Infile = 0, *Outfile = 0, *Autoload_cmd = 0;
static int Multivolume = 0, Memlock = 0, Sendout = 0, PrefixLen;
#ifdef HAVE_LIBMHASH
static MHASH MD5hash;
#elif defined HAVE_LIBMD5
static MD5_CTX md5ctxt;
#elif defined HAVE_LIBCRYPTO
static MD5_CTX md5ctxt;
#endif
static sem_t Dev2Buf, Buf2Dev;
static pthread_cond_t
	PercLow = PTHREAD_COND_INITIALIZER,	/* low watermark */
	PercHigh = PTHREAD_COND_INITIALIZER;	/* high watermark */
static pthread_mutex_t
	TermMut = PTHREAD_MUTEX_INITIALIZER,	/* prevents statusThread from interfering with request*Volume */
	LowMut = PTHREAD_MUTEX_INITIALIZER,
	HighMut = PTHREAD_MUTEX_INITIALIZER;
static int Log = -1;
static int Terminal = 1;
static struct timeval Starttime;


#ifdef DEBUG
static void debugmsg(const char *msg, ...)
{
	if (Verbose >= 5) {
		va_list val;
		int num;

		va_start(val,msg);
		num = vsnprintf(Msgbuf,MSGSIZE,msg,val);
		assert(num < MSGSIZE);
		(void) write(Log,Msg,num + PrefixLen);
		va_end(val);
	}
}
#else
#define debugmsg
#endif

static void infomsg(const char *msg, ...)
{
	if (Verbose >= 4) {
		va_list val;
		int num;

		va_start(val,msg);
		num = vsnprintf(Msgbuf,MSGSIZE,msg,val);
		assert(num < MSGSIZE);
		(void) write(Log,Msg,num + PrefixLen);
		va_end(val);
	}
}

static void warningmsg(const char *msg, ...)
{
	if (Verbose >= 3) {
		va_list val;
		int num = 9;

		va_start(val,msg);
		(void) memcpy(Msgbuf,"warning: ",9);
		num += vsnprintf(Msgbuf+9,MSGSIZE-9,msg,val);
		assert(num < MSGSIZE);
		(void) write(Log,Msg,num + PrefixLen);
		va_end(val);
	}
}

static void errormsg(const char *msg, ...)
{
	ErrorOccured = 1;
	if (Verbose >= 2) {
		va_list val;
		int num = 7;

		va_start(val,msg);
		(void) memcpy(Msgbuf,"error: ",7);
		num += vsnprintf(Msgbuf+7,MSGSIZE-7,msg,val);
		assert(num < MSGSIZE);
		(void) write(Log,Msg,num + PrefixLen);
		va_end(val);
	}
}


static void fatal(const char *msg, ...)
{
	if (Verbose >= 1) {
		va_list val;
		int num = 7;

		va_start(val,msg);
		(void) memcpy(Msgbuf,"fatal: ",7);
		num += vsnprintf(Msgbuf+7,MSGSIZE-7,msg,val);
		assert(num < MSGSIZE);
		(void) write(Log,Msg,num + PrefixLen);
		va_end(val);
	}
	exit(EXIT_FAILURE);
}



static char *kb2str(double v)
{
	const char *dim = "kMGT", *f;
	static char s[8];

	while (v > 10000) {
		v /= 1024.0;
		++dim;
		if (*dim == 0) {
			v *= 1024.0*1024.0*1024.0*1024.0;
			break;
		}
	}
	if (v < 0)
		f = " ??? ";
	else if (v < 100)
		f = "%4.1f %c";
	else if (v < 10000) {
		v = rint(v);
		f = "%4.0f %c";
	} else
		f = "%5.lg ";
	sprintf(s,f,v,*dim);
	return s;
}



static void summary(unsigned long long numb, double secs)
{
	int h = (int) secs/3600, m = (int) (secs - h * 3600)/60;
	double av = (double)(numb >>= 10)/secs;
	char buf[256], *msg = buf;
	
	secs -= m * 60 + h * 3600;
	msg += sprintf(msg,"\nsummary: %sByte in ",kb2str(numb));
	if (h)
		msg += sprintf(msg,"%d h %02d min ",h,m);
	else if (m)
		msg += sprintf(msg,"%2d min ",m);
	msg += sprintf(msg,"%02.1f sec - average of %sB/s",secs,kb2str(av));
	if (EmptyCount)
		msg += sprintf(msg,", %dx empty",EmptyCount);
	if (FullCount)
		msg += sprintf(msg,", %dx full",FullCount);
	*msg++ = '\n';
	*msg = 0;
#ifdef HAVE_LIBMHASH
	if (Hash) {
		unsigned char hashvalue[16];
		int i;
		
		mhash_deinit(MD5hash,hashvalue);
		msg += sprintf(msg,"md5 hash:");
		for (i = 0; i < 16; ++i)
			msg += sprintf(msg," %02x",hashvalue[i]);
		*msg++ = '\n';
	}
#elif defined HAVE_LIBMD5
	if (Hash) {
		unsigned char hashvalue[16];
		int i;
		
		MD5Final(hashvalue,&md5ctxt);
		msg += sprintf(msg,"md5 hash:");
		for (i = 0; i < 16; ++i)
			msg += sprintf(msg," %02x",hashvalue[i]);
		*msg++ = '\n';
	}
#elif defined HAVE_LIBCRYPTO
	if (Hash) {
		unsigned char hashvalue[16];
		int i;
		
		MD5_Final(hashvalue,&md5ctxt);
		msg += sprintf(msg,"md5 hash:");
		for (i = 0; i < 16; ++i)
			msg += sprintf(msg," %02x",hashvalue[i]);
		*msg++ = '\n';
	}
#endif
	(void) write(STDERR_FILENO,buf,strlen(buf));
}



static RETSIGTYPE sigHandler(int signr)
{
	switch (signr) {
	case SIGINT:
	case SIGTERM:
		ErrorOccured = 1;
		Terminate = 1;
		break;
	default:
		(void) raise(SIGABRT);
	}
}



/* Thread-safe replacement for usleep. Argument must be a whole
 * number of microseconds to sleep.
 */
static int mt_usleep(unsigned long sleep_usecs)
{
	struct timespec tv;
	tv.tv_sec = sleep_usecs / 1000000;
	tv.tv_nsec = (sleep_usecs % 1000000) * 1000;

	do {
		/* Sleep for the time specified in tv. If interrupted by a
		 * signal, place the remaining time left to sleep back into tv.
		 */
		if (0 == nanosleep(&tv, &tv)) 
			return 0;
	} while (errno == EINTR);
	return -1;
}



static void statusThread(void) 
{
	struct timeval last, now;
	double in = 0, out = 0, total, diff, fill;
	unsigned long long lin = 0, lout = 0;
	int unwritten = 1;	/* assumption: initially there is at least one unwritten block */
	int ret;

	last = Starttime;
#ifdef __alpha
	(void) mt_usleep(1000);	/* needed on alpha (stderr fails with fpe on nan) */
#endif
	while (!(Finish && (unwritten == 0)) && (Terminate == 0)) {
		int err,nw;
		char buf[256], *b = buf;
		(void) mt_usleep(500000);
		(void) gettimeofday(&now,0);
		diff = now.tv_sec - last.tv_sec + (double) (now.tv_usec - last.tv_usec) / 1000000;
		err = pthread_mutex_lock(&TermMut);
		assert(0 == err);
		err = sem_getvalue(&Buf2Dev,&unwritten);
		assert(0 == err);
		fill = (double)unwritten / (double)Numblocks * 100.0;
		in = ((Numin - lin) * Blocksize) >> 10;
		in /= diff;
		out = ((Numout - lout) * Blocksize) >> 10;
		out /= diff;
		lin = Numin;
		lout = Numout;
		last = now;
		total = (Numout * Blocksize) >> 10;
		fill = (fill < 0) ? 0 : fill;
		b += sprintf(b,"\rin @ %sB/s",kb2str(in));
		b += sprintf(b,", out @ %sB/s",kb2str(out));
		b += sprintf(b,", %sB total, buffer %3.0f%% full",kb2str(total),fill);
		nw = write(STDERR_FILENO,buf,strlen(buf));
		err = pthread_mutex_unlock(&TermMut);
		assert(0 == err);
		if (nw == -1)	/* stop trying to print status messages after a write error */
			break;
	}
	if (Terminate) {
		infomsg("\nterminating...\n");
		(void) pthread_cancel(Writer);
		(void) pthread_cancel(Reader);
	}
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
}


static inline long long timediff(struct timeval *restrict t1, struct timeval *restrict t2)
{
	long long tdiff;
	tdiff = (t1->tv_sec - t2->tv_sec) * 1000000;
	tdiff += t1->tv_usec - t2->tv_usec;
	return tdiff;
}


static long long enforceSpeedLimit(unsigned long long limit, long long num, struct timeval *last)
{
	static long long minwtime = 0, ticktime;
	struct timeval now;
	long long tdiff;
	double dt;
	
	if (minwtime == 0) {
		ticktime = 1000000 / sysconf(_SC_CLK_TCK);
	}
	num += Blocksize;
	(void) gettimeofday(&now,0);
	tdiff = timediff(&now,last);
	assert(tdiff >= 0);
	if (num < 0)
		return num;
	dt = (double)tdiff * 1E-6;
	if (((double)num/dt) > (double)limit) {
		double req = (double)num/limit - dt;
		long long w = (long long) (req * 1E6);
		if (w > ticktime) {
			long long slept, stime;
			stime = w / ticktime;
			stime *= ticktime;
			debugmsg("enforceSpeedLimit(%lld,%lld): sleeping for %lld usec\n",limit,num,stime);
			(void) mt_usleep(stime);
			(void) gettimeofday(last,0);
			slept = timediff(last,&now);
			assert(slept >= 0);
			debugmsg("enforceSpeedLimit(): slept for %lld usec\n",slept);
			slept -= stime;
			if (slept >= 0)
				return -(long long)(limit * (double)slept * 1E-6);
			return 0;
		} else {
			/* 
			 * Sleeping now would cause too much of a slowdown. So
			 * we defer this sleep until the sleeping time is
			 * longer than the tick time. Like this we can stay as
			 * close to the speed limit as possible.
			 */
			return num;
		}
	} else if (tdiff > minwtime) {
		(void) gettimeofday(last,0);
		return 0;
	}
	return num;
}


static void requestInputVolume(void)
{
	char cmd_buf[15+strlen(Infile)];
	const char *cmd;

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
			int err;
			char c = 0, msg[] = "\ninsert next volume and press return to continue...";
			err = pthread_mutex_lock(&TermMut);
			assert(0 == err);
			if (-1 == write(STDERR_FILENO,msg,sizeof(msg))) {
				errormsg("error accessing controlling terminal for manual volume change request: %s\nConsider using autoload option, when running mbuffer without terminal.\n",strerror(errno));
				Terminate = 1;
				pthread_exit((void *) -1);
			}
			do {
				if (-1 == read(STDERR_FILENO,&c,1) && (errno != EINTR)) {
					errormsg("error accessing controlling terminal for manual volume change request: %s\nConsider using autoload option, when running mbuffer without terminal.\n",strerror(errno));
					Terminate = 1;
					pthread_exit((void *) -1);
				}
			} while (c != '\n');
			err = pthread_mutex_unlock(&TermMut);
			assert(0 == err);
		}
		if (-1 == (In = open(Infile,O_RDONLY|LARGEFILE)))
			errormsg("could not reopen input: %s\n",strerror(errno));
#ifdef __sun
		if (-1 == directio(In,DIRECTIO_ON))
			infomsg("direct I/O hinting failed for input: %s\n",strerror(errno));
#endif
	} while (In == -1);
	Multivolume--;
	if (Terminal && ! Autoloader) {
		char msg[] = "\nOK - continuing...\n";
		(void) write(STDERR_FILENO,msg,sizeof(msg));
	}
}



static void *inputThread(void *ignored)
{
	int fill = 0;
	unsigned long long num, at = 0;
	long long xfer = 0;
	const double startread = StartRead, startwrite = StartWrite;
	struct timeval last;

	(void) gettimeofday(&last,0);
	assert(ignored == 0);
	infomsg("inputThread: starting...\n");
	for (;;) {
		int err;

		err = pthread_mutex_lock(&LowMut);
		assert(err == 0);
		err = sem_getvalue(&Buf2Dev,&fill);
		assert(err == 0);
		if ((startread < 1) && (fill == Numblocks - 1)) {
			debugmsg("inputThread: buffer full, waiting for it to drain.\n");
			err = pthread_cond_wait(&PercLow,&LowMut);
			assert(err == 0);
			++FullCount;
			debugmsg("inputThread: low watermark reached, continuing...\n");
		}
		err = pthread_mutex_unlock(&LowMut);
		assert(err == 0);
		err = sem_wait(&Dev2Buf); /* Wait for one or more buffer blocks to be free */
		assert(err == 0);
		if (Terminate) {	/* for async termination requests */
			(void) pthread_cancel(Writer);
			
			debugmsg("inputThread: terminating upon termination request...\n");
			if (-1 == close(In))
				errormsg("error closing input: %s\n",strerror(errno));
			pthread_exit(0);
			return 0;	/* just to make lint happy... */
		}
		num = 0;
		do {
			int in;
			in = read(In,Buffer[at] + num,Blocksize - num);
			debugmsg("inputThread: read(In, Buffer[%llu] + %llu, %llu) = %d\n", at, num, Blocksize - num, in);
			if ((!in) && (Terminal||Autoloader) && (Multivolume)) {
				requestInputVolume();
			} else if (-1 == in) {
				errormsg("inputThread: error reading at offset %llx: %s\n",Numin*Blocksize,strerror(errno));
				Finish = 1;
				if (num) {
					Rest = num;
					err = sem_post(&Buf2Dev);
					assert(err == 0);
				}
				err = pthread_mutex_lock(&HighMut);
				assert(err == 0);
				err = pthread_cond_signal(&PercHigh);
				assert(err == 0);
				err = pthread_mutex_unlock(&HighMut);
				assert(err == 0);
				infomsg("inputThread: exiting...\n");
				pthread_exit((void *) -1);
			} else if (0 == in) {
				Rest = num;
				Finish = 1;
				err = sem_post(&Buf2Dev);
				assert(err == 0);
				debugmsg("inputThread: last block has %d bytes\n",num);
				#ifdef HAVE_LIBMHASH
				if (Hash)
					mhash(MD5hash,Buffer[at],num);
				#elif defined HAVE_LIBMD5
				if (Hash)
					MD5Update(&md5ctxt,(unsigned char *)Buffer[at],num);
				#elif defined HAVE_LIBCRYPTO
				if (Hash)
					MD5_Update(&md5ctxt,Buffer[at],num);
				#endif
				err = pthread_mutex_lock(&HighMut);
				assert(err == 0);
				err = pthread_cond_signal(&PercHigh);
				assert(err == 0);
				err = pthread_mutex_unlock(&HighMut);
				assert(err == 0);
				infomsg("inputThread: exiting...\n");
				pthread_exit(0);
			}
			num += in;
		} while (num < Blocksize);
		#ifdef HAVE_LIBMHASH
		if (Hash)
			mhash(MD5hash,Buffer[at],num);
		#elif defined HAVE_LIBMD5
		if (Hash)
			MD5Update(&md5ctxt,(unsigned char *)Buffer[at],num);
		#elif defined HAVE_LIBCRYPTO
		if (Hash)
			MD5_Update(&md5ctxt,Buffer[at],num);
		#endif
		if (MaxReadSpeed)
			xfer = enforceSpeedLimit(MaxReadSpeed,xfer,&last);
		err = pthread_mutex_lock(&HighMut);
		assert(err == 0);
		err = sem_post(&Buf2Dev);
		assert(err == 0);
		err = sem_getvalue(&Buf2Dev,&fill);
		assert(err == 0);
		if (((double) fill / (double) Numblocks) >= startwrite) {
			err = pthread_cond_signal(&PercHigh);
			assert(err == 0);
		}
		err = pthread_mutex_unlock(&HighMut);
		assert(err == 0);
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
		Terminate = 1;
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
			char c = 0, msg[] = "\nvolume full - insert new media and press return when ready...\n";
			if (Terminal == 0) {
				errormsg("End of volume, but not end of input.\n"
					"Specify an autoload command, if you are working without terminal.\n");
				Terminate = 1;
				pthread_exit((void *) -1);
			}
			err = pthread_mutex_lock(&TermMut);
			assert(0 == err);
			if (-1 == write(STDERR_FILENO,msg,sizeof(msg))) {
				errormsg("error accessing controlling terminal for manual volume change request: %s\nConsider using autoload option, when running mbuffer without terminal.\n",strerror(errno));
				Terminate = 1;
				pthread_exit((void *) -1);
			}
			do {
				if (-1 == read(STDERR_FILENO,&c,1) && (errno != EINTR)) {
					errormsg("error accessing controlling terminal for manual volume change request: %s\nConsider using autoload option, when running mbuffer without terminal.\n",strerror(errno));
					Terminate = 1;
					pthread_exit((void *) -1);
				}
			} while (c != '\n');
			err = pthread_mutex_unlock(&TermMut);
			assert(0 == err);
		}
		if (-1 == (Out = open(Outfile,Nooverwrite|O_CREAT|O_WRONLY|O_TRUNC|OptSync|LARGEFILE,0666)))
			errormsg("error reopening output file: %s\n",strerror(errno));
#ifdef __sun
		if (-1 == directio(Out,DIRECTIO_ON))
			infomsg("direct I/O hinting failed for output: %s\n",strerror(errno));
#endif
	} while (-1 == Out);
	infomsg("continuing with next volume\n");
	if (Terminal && ! Autoloader) {
		char msg[] = "\nOK - continuing...\n";
		(void) write(STDERR_FILENO,msg,sizeof(msg));
	}
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



static void terminateOutputThread(void)
{
	infomsg("outputThread: syncing...\n");
	while (0 != fsync(Out)) {
		if (errno == EINTR) {
			continue;
		} else if (errno == EINVAL) {
			warningmsg("output does not support syncing: omitted.\n");
			break;
		} else {
			fatal("error syncing: %s\n",strerror(errno));
		}
	}
	infomsg("outputThread: finished - exiting...\n");
	if (-1 == close(Out))
		errormsg("error closing output: %s\n",strerror(errno));
	pthread_exit(0);
}



static void *outputThread(void *ignored)
{
	unsigned at = 0;
	int fill = 0;
	const double startwrite = StartWrite, startread = StartRead;
	unsigned long long blocksize = Blocksize;
	long long xfer = 0;
	struct timeval last;

	/* initialize last to 0, because we don't want to wait initially */
	(void) gettimeofday(&last,0);
	assert(ignored == 0);
	infomsg("\noutputThread: starting...\n");
	for (;;) {
		unsigned long long rest = blocksize;
		int err;

		err = pthread_mutex_lock(&HighMut);
		assert(err == 0);
		err = sem_getvalue(&Buf2Dev,&fill);
		assert(err == 0);
		if ((fill == 0) && (startwrite > 0)) {
			debugmsg("outputThread: buffer empty, waiting for it to fill\n");
			err = pthread_cond_wait(&PercHigh,&HighMut);
			assert(err == 0);
			++EmptyCount;
			debugmsg("outputThread: high watermark reached, continuing...\n");
		}
		err = pthread_mutex_unlock(&HighMut);
		assert(err == 0);
		err = sem_wait(&Buf2Dev);
		assert(err == 0);
		if (Terminate) {
			(void) pthread_cancel(Reader);
			debugmsg("outputThread: terminating upon termination request...\n");
			if (-1 == close(Out)) 
				errormsg("error closing output: %s\n",strerror(errno));;
			pthread_exit(0);
		}
		if (Finish) {
			debugmsg("outputThread: inputThread finished, %d blocks remaining\n",fill);
			err = sem_getvalue(&Buf2Dev,&fill);
			assert(err == 0);
			if ((0 == Rest) && (0 == fill)) {
				infomsg("outputThread: finished - exiting...\n");
				pthread_exit(0);
			} else if (0 == fill) {
				blocksize = rest = Rest;
				debugmsg("outputThread: last block has %d bytes\n",Rest);
			}
		}
		/* switch output volume if -D <size> has been reached */
		if ( (OutVolsize != 0) && (Numout > 0) && (Numout % (OutVolsize/Blocksize)) == 0 ) {
			/* Sleep to let status thread "catch up" so that the displayed total is a multiple of OutVolsize */
			(void) mt_usleep(500000);
			requestOutputVolume();
		}
		do {
			/* use Outsize which could be the blocksize of the device (option -d) */
			int num;
#ifdef HAVE_SENDFILE
			if (Sendout) {
				off_t baddr = (off_t) Buffer[at] + blocksize - rest;
				num = sendfile(Out,SFV_FD_SELF,&baddr,rest > Outsize ? Outsize : rest);
#ifdef DEBUG
				debugmsg("outputThread: sendfile(Out, SFV_FD_SELF, Buffer[%d] + %llu, %llu) = %d\n", at, blocksize - rest, rest > Outsize ? Outsize : rest, num);
#endif
			} else
#endif
			{
				num = write(Out,Buffer[at] + blocksize - rest, rest > Outsize ? Outsize : rest);
#ifdef DEBUG
				debugmsg("outputThread: write(Out, Buffer[%d] + %llu, %d) = %d\t(rest = %d)\n", at, blocksize - rest, rest > Outsize ? Outsize : rest, num, rest);
#endif
			}
			if ((-1 == num) && (Terminal||Autoloader) && ((errno == ENOMEM) || (errno == ENOSPC))) {
				/* request a new volume - but first check
				 * wheather we are really at the
				 * end of the device */
				checkIncompleteOutput();
				continue;
			} else if (-1 == num) {
				errormsg("outputThread: error writing at offset %llx: %s\n",Blocksize*Numout+blocksize-rest,strerror(errno));
				Terminate = 1;
				err = sem_post(&Dev2Buf);
				assert(err == 0);
				pthread_exit((void *) -1);
			}
			rest -= num;
		} while (rest > 0);
		err = sem_post(&Dev2Buf);
		assert(err == 0);
		if (MaxWriteSpeed) {
			xfer = enforceSpeedLimit(MaxWriteSpeed,xfer,&last);
		}
		if (Pause)
			(void) mt_usleep(Pause);
		at++;
		if (Numblocks == at)
			at = 0;
		err = pthread_mutex_lock(&LowMut);
		assert(err == 0);
		err = sem_getvalue(&Buf2Dev,&fill);
		assert(err == 0);
		if (Finish && (0 == fill)) {
			terminateOutputThread();
			return 0;	/* make lint happy */
		}
		if ((startread < 1) && ((double)fill / (double)Numblocks) < startread) {
			err = pthread_cond_signal(&PercLow);
			assert(err == 0);
		}
		err = pthread_mutex_unlock(&LowMut);
		assert(err == 0);
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
#ifdef HAVE_HSTRERROR
			fatal("could not resolve server hostname: %s\n",hstrerror(h_errno));
#else
			fatal("could not resolve server hostname: error code %d\n",h_errno);
#endif
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
			infomsg("connection accepted\n");
			return;
		}
		for (p = h->h_addr_list; *p; ++p) {
			if (0 == memcmp(&caddr.sin_addr,*p,h->h_length)) {
				infomsg("connection accepted\n");
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
#ifdef HAVE_HSTRERROR
		fatal("could not resolve server hostname: %s\n",hstrerror(h_errno));
#else
		fatal("could not resolve server hostname: error code %d\n",h_errno);
#endif
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
	if (1 < sscanf(argv[*c],"%[0-9a-zA-Z_.-]:%hu",tmphost,port)) {
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
		"Copyright 2001-2008 - T. Maier-Komor\n"\
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
#ifdef _SC_PHYS_PAGES
		"-m <size>  : use buffer of a total of <size> bytes\n"
#endif
#ifdef _POSIX_MEMLOCK_RANGE
		"-L         : lock buffer in memory (unusable with file based buffers)\n"
#endif
#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
		"-d         : use blocksize of device for output\n"
#endif
		"-D <size>  : assumed device size (default: infinite)\n"
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
#if defined HAVE_LIBCRYPTO || defined HAVE_LIBMD5 || defined HAVE_LIBMHASH
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
				fatal("invalid value for number of bytes\n");
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
	long mxnrsem;
	int err, optMset = 0, optSset = 0, optBset = 0, c, fl;
	sigset_t       signalSet;
#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
	struct stat st;
	int setOutsize = 0;
#endif
	unsigned short netPortIn = 0;
	const char *server = 0, *client = 0;
	unsigned short netPortOut = 0;
	char *argv0 = strdup(argv[0]), *progname;
	
	progname = basename(argv0);
	PrefixLen = strlen(progname) + 2;
	Msg = malloc(PrefixLen + MSGSIZE);
	(void) strcpy(Msg,progname);
	Msg[PrefixLen - 2] = ':';
	Msg[PrefixLen - 1] = ' ';
	Msgbuf = Msg + PrefixLen;
	Log = STDERR_FILENO;
	for (c = 1; c < argc; c++) {
		if (!argcheck("-s",argv,&c)) {
			Blocksize = Outsize = calcint(argv,c,Blocksize);
			optSset = 1;
			debugmsg("Blocksize = %llu\n",Blocksize);
			if (Blocksize < 100)
				fatal("cannot set blocksize as percentage of total physical memory\n");
#ifdef _SC_PHYS_PAGES
		} else if (!argcheck("-m",argv,&c)) {
			totalmem = calcint(argv,c,totalmem);
			optMset = 1;
			if (totalmem < 100) {
				long pgsz, nump;
				pgsz = sysconf(_SC_PAGESIZE);
				assert(pgsz > 0);
				nump = sysconf(_SC_PHYS_PAGES);
				assert(nump > 0);
				debugmsg("total # of phys pages: %li (pagesize %li)\n",nump,pgsz);
				totalmem = ((unsigned long long) nump * pgsz * totalmem) / 100 ;
			}
			debugmsg("totalmem = %llu\n",totalmem);
#endif
		} else if (!argcheck("-b",argv,&c)) {
			Numblocks = (atoi(argv[c])) ? ((unsigned long long) atoll(argv[c])) : Numblocks;
			optBset = 1;
			debugmsg("Numblocks = %llu\n",Numblocks);
		} else if (!argcheck("-d",argv,&c)) {
#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
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
			Log = open(argv[c],O_WRONLY|O_TRUNC|O_CREAT,0666);
			if (-1 == Log) {
				Log = STDERR_FILENO;
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
			if (1 != sscanf(argv[c],"%lf",&StartWrite))
				StartWrite = 0;
			StartWrite /= 100;
			if ((StartWrite > 1) || (StartWrite <= 0))
				fatal("error in argument -P: must be bigger than 0 and less or equal 100");
			debugmsg("StartWrite = %1.2lf\n",StartWrite);
		} else if (!argcheck("-p",argv,&c)) {
			if (1 == sscanf(argv[c],"%lf",&StartRead))
				StartRead /= 100;
			else
				StartRead = 1.0;
			if ((StartRead >= 1) || (StartRead < 0))
				fatal("error in argument -p: must be bigger or equal to 0 and less than 100");
			debugmsg("StartRead = %1.2lf\n",StartRead);
#ifdef _POSIX_MEMLOCK_RANGE
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
				errormsg("error initializing md5 hasing - will not generate hash...\n");
				Hash = 0;
			}
#elif defined HAVE_LIBMD5
			Hash = 1;
			MD5Init(&md5ctxt);
#elif defined HAVE_LIBCRYPTO
			Hash = 1;
			MD5_Init(&md5ctxt);
#else
			warningmsg("md5 hash support has not been compiled in!");
#endif
		} else if (!argcheck("-D",argv,&c)) {
			OutVolsize = calcint(argv,c,0);
			debugmsg("OutVolsize = %llu\n",OutVolsize);
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
		fatal("setting both low watermark and high watermark doesn't make any sense...\n");
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

	/* SPW: Volsize consistency checking */
	if (OutVolsize && !Outfile)
		fatal("Setting OutVolsize without an output device doesn't make sense!\n");
	if ( (OutVolsize != 0) && (OutVolsize < Blocksize) )
		/* code assumes we can write at least one block */
		fatal("If non-zero, OutVolsize must be at least as large as the buffer blocksize (%llu)!\n",Blocksize);
	/* SPW END */

	/* check that we stay within system limits */
	mxnrsem = sysconf(_SC_SEM_VALUE_MAX);
	if (-1 == mxnrsem) {
		warningmsg("unable to determine maximum value of semaphores\n");
	} else if (Numblocks > (unsigned long long) mxnrsem)
		fatal("cannot allocate more than %d blocks.\nThis is a system dependent limit, depending on the maximum semaphore value.\nPlease choose a bigger block size.\n",mxnrsem);

	if (Blocksize * Numblocks > SSIZE_MAX)
		fatal("Cannot address so much memory (%lld*%lld=%lld>%lld).\n",Blocksize,Numblocks, Blocksize*Numblocks,SSIZE_MAX);
	/* create buffer */
	Buffer = (char **) memalign(sysconf(_SC_PAGESIZE),Numblocks * sizeof(char *));
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
	for (c = 1; c < Numblocks; c++) {
		Buffer[c] = Buffer[0] + Blocksize * c;
		*Buffer[c] = 0;	/* touch every block before locking */
	}

#ifdef _POSIX_MEMLOCK_RANGE
	if (Memlock) {
		uid_t uid;
#ifndef HAVE_SETEUID
#define seteuid setuid
#endif
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

	debugmsg("opening streams...\n");
	if (Infile) {
		if (-1 == (In = open(Infile,O_RDONLY)))
			fatal("could not open input file: %s\n",strerror(errno));
	} else if (netPortIn) {
		openNetworkInput(client,netPortIn);
	} else
		In = fileno(stdin);
#ifdef __sun
	if (-1 == directio(In,DIRECTIO_ON))
		infomsg("direct I/O hinting failed for input: %s\n",strerror(errno));
#endif
	if (Outfile) {
		if (-1 == (Out = open(Outfile,Nooverwrite|O_CREAT|O_WRONLY|O_TRUNC|OptSync,0666)))
			fatal("could not open output file: %s\n",strerror(errno));
	} else if (netPortOut) {
		openNetworkOutput(server,netPortOut);
	} else
		Out = fileno(stdout);
#ifdef __sun
	if (-1 == directio(Out,DIRECTIO_ON))
		infomsg("direct I/O hinting failed for output: %s\n",strerror(errno));
#endif

#ifdef HAVE_STRUCT_STAT_ST_BLKSIZE
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
	debugmsg("checking if we have a controlling terminal...\n");
	fl = fcntl(STDERR_FILENO,F_GETFL);
	err = fcntl(STDERR_FILENO,F_SETFL,fl | O_NONBLOCK);
	assert(err == 0);
	if ((read(STDERR_FILENO,&c,1) == -1) && (errno != EAGAIN)) {
		int tty = open("/dev/tty",O_RDWR);
		if (-1 == tty) {
			Terminal = 0;
			if (Autoloader == 0)
				warningmsg("No controlling terminal and no autoloader command specified.\n");
		} else {
			Terminal = 1;
			err = dup2(tty,STDERR_FILENO);
			assert(err != -1);
		}
	}
	err = fcntl(STDERR_FILENO,F_SETFL,fl);
	assert(err == 0);

	debugmsg("registering signals...\n");
	if (SIG_ERR == signal(SIGINT,sigHandler))
		warningmsg("error registering new SIGINT handler: %s\n",strerror(errno));
	if (SIG_ERR == signal(SIGTERM,sigHandler))
		warningmsg("error registering new SIGINT handler: %s\n",strerror(errno));

	debugmsg("starting threads...\n");
	(void) gettimeofday(&Starttime,0);
	err = sigfillset(&signalSet);
	assert(0 == err);
	(void) pthread_sigmask(SIG_BLOCK, &signalSet, NULL);
	err = pthread_create(&Writer,0,&outputThread,0);
	assert(0 == err);
	err = pthread_create(&Reader,0,&inputThread,0);
	assert(0 == err);
	(void) pthread_sigmask(SIG_UNBLOCK, &signalSet, NULL);
	if (Status)
		statusThread();
	else {
		struct timeval now;
		double diff;

		(void) pthread_join(Reader,0);
		(void) pthread_join(Writer,0);
		(void) gettimeofday(&now,0);
		diff = now.tv_sec - Starttime.tv_sec + (double) now.tv_usec / 1000000 - (double) Starttime.tv_usec / 1000000;
		summary(Numout * Blocksize + Rest, diff);
	}
	if (ErrorOccured)
		exit(EXIT_FAILURE);
	exit(EXIT_SUCCESS);
}

/* vim:tw=0
 */
