# Makefile for the Embedded Neo4j Python bindings documentation.
#

# Project Configuration
project_name               = manual
language                   = en

# Minimal setup
target                     = target
build_dir                  = $(CURDIR)/$(target)
config_dir                 = $(CURDIR)/docs/conf
tools_dir                  = $(build_dir)/tools
make_dir                   = $(tools_dir)/make

include $(make_dir)/context-manual.make

