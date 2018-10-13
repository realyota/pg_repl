MODULE_big = ddl_repl
OBJS = ddl_repl.o 
EXTENSION = ddl_repl
DATA = ddl_repl--1.0.sql
REGRESS = ddl_repl

PG_CPPFLAGS = -lpq

ifdef NO_PGXS
subdir = contrib/ddl_repl
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
else
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
endif

