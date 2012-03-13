OUTPUT=darshan-runtime.pdf darshan-runtime.html

all:: $(OUTPUT)

%.pdf: %.txt
	a2x -f pdf $<

%.html: %.txt
	a2x -f xhtml $<

clean::
	rm -f $(OUTPUT)
