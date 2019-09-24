# contrib/walreader/Makefile

MODULE_big = walreader
OBJS = walreader.o $(WIN32RES)

EXTENSION = walreader
DATA = walreader--1.0.sql
PGFILEDESC = "walreader - read WAL via SQL"

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/walreader
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif
