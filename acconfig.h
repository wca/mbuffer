#ifndef CONFIG_H
#define CONFIG_H

/* package */
#undef PACKAGE

/* version of mbuffer */
#undef VERSION

/* localization support */
#undef LC_ALL
#undef LOCALEDIR
#undef ENABLE_NLS
#undef HAVE_CATGETS
#undef HAVE_GETTEXT 
#undef HAVE_LC_MESSAGES
#undef HAVE_STPCPY

/* Define if you want debugging messages enabled. */
#undef DEBUG

/* Undefine if you want asserts enabled. */
#undef NDEBUG

/* multi-support is enabled by default now */
#undef MULTIVOLUME

/* networking code is still BETA */
#undef NETWORKING

/* needed to include experimental code */
#undef EXPERIMENTAL

/* Define if you have a working `mmap' system call.  */
#undef HAVE_MMAP

/* Define as the return type of signal handlers (int or void).  */
#undef RETSIGTYPE


#endif
