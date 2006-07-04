CC		= cc
DEFS		= -DHAVE_CONFIG_H
CFLAGS		= -g -O2 -Wall -O $(DEFS)
LDFLAGS		= 
LIBS		= -lssl -lpthread 

prefix		= /usr/local
exec_prefix     = /usr/local
bindir          = ${exec_prefix}/bin
mandir		= ${prefix}/man/man1

RM		= /bin/rm
INSTALL		= /usr/bin/install -c

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
	config.status Makefile mbuffer.1 core

install:
	-$(INSTALL) -d $(DESTDIR)$(bindir)
	$(INSTALL) $(TARGET) $(DESTDIR)$(bindir)
	$(INSTALL) mbuffer.1 $(DESTDIR)$(mandir)
