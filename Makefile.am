SUBDIRS		= intl po
bin_PROGRAMS    = mbuffer
mbuffer_SOURCES = mbuffer.c
LIBS		= @INTLLIBS@ @LIBS@
mbuffer_LDADD	= $(LIBS)
localedir       = $(datadir)/locale
INCLUDES        = -Iintl -DLOCALEDIR=\"$(localedir)\"
DISTFILES	= ABOUT-NLS

top_srcdir	= @top_srcdir@

man_MANS	= mbuffer.1
