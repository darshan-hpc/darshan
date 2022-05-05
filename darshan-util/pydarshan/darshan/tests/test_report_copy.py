## Check if copy.copy is sufficient (ok on this level.. but!)
#id(r1.records['POSIX']) == id(combined.records['POSIX'])
## False means: The dictionary holding POSIX records are not the same (this does not extend to referenced values (here log-records)!)
## Expect false
#
#
## Check if copy.copy is sufficient (potential conflict for this level?)
## The question has to be, if we assume people to modify a log record
## I'd tend to say no, and document that with a warning somewhere
#id(r1.records['POSIX'][0]) == id(combined.records['POSIX'][0])
## Expect false
