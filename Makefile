CC		= cc
DEFS		= -DHAVE_CONFIG_H
CFLAGS		= -g -O $(DEFS)
LDFLAGS		= 
LIBS		= -lmd5 -lsendfile -lresolv -lnsl -lsocket -lrt -lpthread 

prefix		= /usr/local
exec_prefix     = /usr/local
bindir          = ${exec_prefix}/bin
mandir		= ${prefix}/man/man1

RM		= /usr/bin/rm
INSTALL		= /opt/sfw/bin/ginstall -c

TARGET		= mbuffer
SOURCES		= mbuffer.c
OBJECTS		= $(SOURCES:.c=.o)

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) $(LDFLAGS) $(LIBS) $(OBJECTS) -o $@

clean:
	-$(RM) $(OBJECTS)

distclean: clean
	-$(RM) $(TARGET) config.h config.log \
	config.status Makefile mbuffer.1

install:
	$(INSTALL) -d $(DESTDIR)$(bindir)
	$(INSTALL) $(TARGET) $(DESTDIR)$(bindir)
	$(INSTALL) mbuffer.1 $(DESTDIR)$(mandir)
