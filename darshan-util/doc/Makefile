PDF=darshan-util.pdf

all:: $(PDF)

%.pdf: %.txt
	a2x -f pdf $<

clean::
	rm -f $(PDF)
