OUTPUT=darshan-util.pdf darshan-util.html

all:: $(OUTPUT)

%.pdf: %.txt
	a2x -f pdf $<

%.html: %.txt
	asciidoc $<

clean::
	rm -f $(OUTPUT)
