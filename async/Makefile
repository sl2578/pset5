SHELL := /bin/sh
ML_CODE		:= $(filter-out %_test.ml, $(wildcard *.ml))
ML_TESTS	:= $(wildcard *_test.ml)

all: 
	# compiles all .ml files in directory
	for file in $(ML_CODE) ; do \
		cs3110 compile $$file ; \
	done

clean: 
	# cleans any previous compiles, in case of corruption
	rm -rf _build

format: 
	# uses an autoformatter to format your code. 
	# Make sure you've saved, as it overwrites the file!
	for file in $(wildcard *.ml) ; do \
		camlp4 $$file -parser o -printer o > new && cat new > $$file ; \
	done

test:
	# compiles and tests all the files that end in "_test.ml"
	for file in $(ML_TESTS) ; do \
		cs3110 compile $$file && cs3110 test $$file ; \
	done