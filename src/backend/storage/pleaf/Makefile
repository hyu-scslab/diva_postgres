#-------------------------------------------------------------------------
#
# Makefile--
#    Makefile for storage/pleaf
#
# IDENTIFICATION
#    src/backend/storage/pleaf/Makefile
#
#-------------------------------------------------------------------------

subdir = src/backend/storage/pleaf
top_builddir = ../../../..
include $(top_builddir)/src/Makefile.global

OBJS = \
	pleaf.o	pleaf_internals.o	pleaf_reader.o pleaf_writer.o \
	pleaf_buf.o pleaf_stack_helper.o \
	pleaf_hash.o pleaf_bufpage.o pleaf_mgr.o

include $(top_srcdir)/src/backend/common.mk
