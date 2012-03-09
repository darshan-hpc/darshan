OUTPUT=darshan-runtime.pdf darshan-runtime.html

all:: $(OUTPUT)

%.pdf: %.txt
	a2x -f pdf $<

%.html: %.txt
	asciidoc $<

clean::
	rm -f $(OUTPUT)
