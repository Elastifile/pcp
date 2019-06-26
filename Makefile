
TARGET:=pcp
HDRS=
SRCS:=pcp.c tar.c
LIBS:=pthread
CFLAGS+=-Wno-missing-braces

# check for git
GIT:=$(shell which git)
ifneq ($(GIT),)
commit=${shell echo `git rev-parse --short HEAD`:`git name-rev HEAD` | tr ' ' -}
endif

OBJS=$(SRCS:%.c=%.o)
_LIBS=${patsubst %,-l %, ${LIBS}}

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) -g -O3 -D _LARGEFILE64_SOURCE -DCOMMIT="${commit}" -Wall -o $@ $(LDFLAGS) $^ ${_LIBS}

pcp32:
	# requires glibc-devel.i686 libaio-devel.i686 libaio.i686 libgcc.i686
	CFLAGS=-m32 make

test:
	@echo "Target: " ${TARGET} " commit: "${commit} " hdrs: " ${HDRS} " srcs: " ${SRCS} " libs: " ${LIBS} " objs: " ${OBJS}

clean:
	rm -f $(OBJS) $(TARGET) *.gz

%.o: %.c $(HDRS)
	$(CC) $(CFLAGS) -c -g -O3 -D _LARGEFILE64_SOURCE -DCOMMIT="${commit}" -Wall -o $@ $<

tar: $(TARGET)
	rev=`./$(TARGET) -v | cut -f 3 -d " " | tr ":" "-"` && echo $$rev && git archive --format=tar --prefix=pcp-$$rev/ HEAD | gzip >pcp-$$rev.tar.gz

.PHONY: indent
indent:
	indent -kr -nut *.c *.h