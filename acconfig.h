#ifndef CONFIG_H
#define CONFIG_H

/* version of mbuffer */
#undef VERSION

/* Define if you want debugging messages enabled. */
#undef DEBUG

/* Undefine if you want asserts enabled. */
#undef NDEBUG

/* multi-support is enabled by default now */
#undef MULTIVOLUME

/* needed to include experimental code */
#undef EXPERIMENTAL

/* Define if you have a working `mmap' system call.  */
#undef HAVE_MMAP

/* Define as the return type of signal handlers (int or void).  */
#undef RETSIGTYPE

#endif
