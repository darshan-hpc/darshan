OUTPUT=darshan-util.pdf darshan-util.html

all:: $(OUTPUT)

%.pdf: %.txt
	a2x -f pdf $<

%.html: %.txt
	a2x -f xhtml $<

clean::
	rm -f $(OUTPUT)
