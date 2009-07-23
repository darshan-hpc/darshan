#!/usr/bin/perl -w
#
#  (C) 2009 by Argonne National Laboratory.
#      See COPYRIGHT in top-level directory.
#

use TeX::Encode;
use Encode;

open(TRACE, $ARGV[0]) || die("can't open $ARGV[0] for processing: $!\n");

$max_access = -1;
$max_access_hash = 0;

while ($line = <TRACE>) {
    chop($line);
    
    if ($line =~ /^\s*$/) {
        # ignore blank lines
    }
    elsif ($line =~ /^#/) {
	if ($line =~ /^# exe: /) {
	    ($junk, $cmdline) = split(':', $line, 2);
            # add escape characters if needed for special characters in
            # command line
            $cmdline = encode('latex', $cmdline);
	}
	if ($line =~ /^# nprocs: /) {
	    ($junk, $nprocs) = split(':', $line, 2);
	    $procreads[$nprocs] = 0;
	}
	if ($line =~ /^# run time: /) {
	    ($junk, $runtime) = split(':', $line, 2);
	}
	if ($line =~ /^# start_time: /) {
	    ($junk, $starttime) = split(':', $line, 2);
	}
	if ($line =~ /^# uid: /) {
	    ($junk, $uid) = split(':', $line, 2);
	}
    }
    else {
	@fields = split(/[\t ]+/, $line);
	$summary{$fields[2]} += $fields[3];

	# record per-process POSIX read count
	if ($fields[2] eq "CP_POSIX_READS") {
	    if ($fields[0] == -1) {
		$procreads[$nprocs] += $fields[3];
	    }
	    else {
		$procreads[$fields[0]] += $fields[3];
	    }
	}

	# record per-proces POSIX write count
	if ($fields[2] eq "CP_POSIX_WRITES") {
	    if ($fields[0] == -1) {
		$procwrites[$nprocs] += $fields[3];
	    }
	    else {
		$procwrites[$fields[0]] += $fields[3];
	    }
	}

        # record access and stride counters
        if ($fields[2] =~ /(^CP_STRIDE.*)/) {
            $tmpfield = $1;

            if(defined $common{$fields[1]}{$tmpfield}) {
                $common{$fields[1]}{$tmpfield} += $fields[3];
            }
            else {
                $common{$fields[1]}{$tmpfield} = $fields[3];
            }
            $common{$fields[1]}{'name'} = $fields[4];
        }
        if ($fields[2] =~ /(^CP_ACCESS.*)/) {
            $tmpfield = $1;
            if(defined $common{$fields[1]}{$tmpfield}) {
                $common{$fields[1]}{$tmpfield} += $fields[3];
            }
            else {
                $common{$fields[1]}{$tmpfield} = $fields[3];
            }
            if(defined $common{$fields[1]}{'totalaccess'}) {
                $common{$fields[1]}{'totalaccess'} += $fields[3];
            }
            else {
                $common{$fields[1]}{'totalaccess'} = $fields[3];
            }

            if($common{$fields[1]}{'totalaccess'} > $max_access) {
                $max_access = $common{$fields[1]}{'totalaccess'};
                $max_access_hash = $fields[1];
            }
        }
    }
}

# print "max_access: $max_access.\n";
# print "max_access_hash: $max_access_hash.\n";

# counts of operations
open(COUNTS, ">counts.dat") || die("error opening output file: $!\n");
print COUNTS "# P=POSIX, MI=MPI-IO indep., MC=MPI-IO coll., R=read, W=write\n";
print COUNTS "# PR, MIR, MCR, PW, MIW, MCW, Popen, Pseek, Pstat\n";
print COUNTS "Read, ", $summary{CP_POSIX_READS}, ", ",
    $summary{CP_INDEP_READS}, ", ", $summary{CP_COLL_READS}, "\n",
    "Write, ", $summary{CP_POSIX_WRITES}, ", ", 
    $summary{CP_INDEP_WRITES}, ", ", $summary{CP_COLL_WRITES}, "\n",
    "Open, ", $summary{CP_POSIX_OPENS}, ", ", $summary{CP_INDEP_OPENS},", ",
    $summary{CP_COLL_OPENS}, "\n",
    "Stat, ", $summary{CP_POSIX_STATS}, ", 0, 0\n",
    "Seek, ", $summary{CP_POSIX_SEEKS}, ", 0, 0\n",
    "Mmap, ", $summary{CP_POSIX_MMAPS}, ", 0, 0\n";
close COUNTS;

# histograms of reads and writes
open (HIST, ">hist.dat") || die("error opening output file: $!\n");
print HIST "# size_range read write\n";
print HIST "0-100, ", $summary{CP_SIZE_READ_0_100}, ", ",
                 $summary{CP_SIZE_WRITE_0_100}, "\n";
print HIST "101-1K, ", $summary{CP_SIZE_READ_100_1K}, ", ",
                 $summary{CP_SIZE_WRITE_100_1K}, "\n";
print HIST "1K-10K, ", $summary{CP_SIZE_READ_1K_10K}, ", ",
                 $summary{CP_SIZE_WRITE_1K_10K}, "\n";
print HIST "10K-100K, ", $summary{CP_SIZE_READ_10K_100K}, ", ",
                 $summary{CP_SIZE_WRITE_10K_100K}, "\n";
print HIST "100K-1M, ", $summary{CP_SIZE_READ_100K_1M}, ", ",
                 $summary{CP_SIZE_WRITE_100K_1M}, "\n";
print HIST "1M-4M, ", $summary{CP_SIZE_READ_1M_4M}, ", ",
                 $summary{CP_SIZE_WRITE_1M_4M}, "\n";
print HIST "4M-10M, ", $summary{CP_SIZE_READ_4M_10M}, ", ",
                 $summary{CP_SIZE_WRITE_4M_10M}, "\n";
print HIST "10M-100M, ", $summary{CP_SIZE_READ_10M_100M}, ", ",
                 $summary{CP_SIZE_WRITE_10M_100M}, "\n";
print HIST "100M-1G, ", $summary{CP_SIZE_READ_100M_1G}, ", ",
                 $summary{CP_SIZE_WRITE_100M_1G}, "\n";
print HIST "1G+, ", $summary{CP_SIZE_READ_1G_PLUS}, ", ",
                 $summary{CP_SIZE_WRITE_1G_PLUS}, "\n";
close HIST;

# sequential and consecutive accesses
open (PATTERN, ">pattern.dat") || die("error opening output file: $!\n");
print PATTERN "# op total sequential consecutive\n";
print PATTERN "Read, ", $summary{CP_POSIX_READS}, ", ",
    $summary{CP_SEQ_READS}, ", ", $summary{CP_CONSEC_READS}, "\n";
print PATTERN "Write, ", $summary{CP_POSIX_WRITES}, ", ",
    $summary{CP_SEQ_WRITES}, ", ", $summary{CP_CONSEC_WRITES}, "\n";
close PATTERN;

# aligned I/O
open (ALIGN, ">align.dat") || die("error opening output file: $!\n");
print ALIGN "# total unaligned_mem unaligned_file align_mem align_file\n";
print ALIGN $summary{CP_POSIX_READS} + $summary{CP_POSIX_WRITES}, ", ",
    $summary{CP_MEM_NOT_ALIGNED}, ", ", $summary{CP_FILE_NOT_ALIGNED}, "\n";
close ALIGN;

# MPI types
open (TYPES, ">types.dat") || die("error opening output file: $!\n");
print TYPES "# type use_count\n";
print TYPES "Named, ", $summary{CP_COMBINER_NAMED}, "\n";
print TYPES "Dup, ", $summary{CP_COMBINER_DUP}, "\n";
print TYPES "Contig, ", $summary{CP_COMBINER_CONTIGUOUS}, "\n";
print TYPES "Vector, ", $summary{CP_COMBINER_VECTOR}, "\n";
print TYPES "HvecInt, ", $summary{CP_COMBINER_HVECTOR_INTEGER}, "\n";
print TYPES "Hvector, ", $summary{CP_COMBINER_HVECTOR}, "\n";
print TYPES "Indexed, ", $summary{CP_COMBINER_INDEXED}, "\n";
print TYPES "HindInt, ", $summary{CP_COMBINER_HINDEXED_INTEGER}, "\n";
print TYPES "Hindexed, ", $summary{CP_COMBINER_HINDEXED}, "\n";
print TYPES "IndBlk, ", $summary{CP_COMBINER_INDEXED_BLOCK}, "\n";
print TYPES "StructInt, ", $summary{CP_COMBINER_STRUCT_INTEGER}, "\n";
print TYPES "Struct, ", $summary{CP_COMBINER_STRUCT}, "\n";
print TYPES "Subarray, ", $summary{CP_COMBINER_SUBARRAY}, "\n";
print TYPES "Darray, ", $summary{CP_COMBINER_DARRAY}, "\n";
print TYPES "F90Real, ", $summary{CP_COMBINER_F90_REAL}, "\n";
print TYPES "F90Complex, ", $summary{CP_COMBINER_F90_COMPLEX}, "\n";
print TYPES "F90Int, ", $summary{CP_COMBINER_F90_INTEGER}, "\n";
print TYPES "Resized, ", $summary{CP_COMBINER_RESIZED}, "\n";
close TYPES;

# generate histogram of process I/O counts
#
# NOTE: NEED TO FILL IN ACTUAL WRITE DATA!!!
#
$minprocread = (defined $procreads[0]) ? $procreads[0] : 0;
$maxprocread = (defined $procreads[0]) ? $procreads[0] : 0;
for ($i=1; $i < $nprocs; $i++) {
    $rdi = (defined $procreads[$i]) ? $procreads[$i] : 0;
    $minprocread = ($rdi > $minprocread) ? $minprocread : $rdi;
    $maxprocread = ($rdi < $maxprocread) ? $maxprocread : $rdi;
}
$minprocread += $procreads[$nprocs];
$maxprocread += $procreads[$nprocs];
# print "$minprocread $maxprocread\n";

@bucket = ( 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 );

for ($i=0; $i < $nprocs; $i++) {
    $mysize = ((defined $procreads[$i]) ? $procreads[$i] : 0) +
	$procreads[$nprocs];
    $mysize -= $minprocread;
    $mybucket = ($mysize > 0) ?
	(($mysize * 10) / ($maxprocread - $minprocread)) : 0;
    $bucket[$mybucket]++;
}

open(IODIST, ">iodist.dat") || die("error opening output file: $!\n");
print IODIST "# bucket n_procs_rd n_procs_wr\n";
print IODIST "# NOTE: WRITES ARE A COPY OF READS FOR NOW!!!\n";

$bucketsize = $maxprocread - $minprocread / 10;
# TODO: do writes also, is dropping a 0 in for now
for ($i=0; $i < 10; $i++) {
    print IODIST $bucketsize * $i + $minprocread, "-",
    $bucketsize * ($i+1) + $minprocread, ", ", $bucket[$i], ", 0\n";
}
close IODIST;

# generate title for summary
($executable, $junk) = split(' ', $cmdline, 2);
@parts = split('/', $executable);
$cmd = $parts[$#parts];

@timearray = localtime($starttime);
$year = $timearray[5] + 1900;
$mon = $timearray[4] + 1;
$mday = $timearray[3];

open(TITLE, ">title.tex") || die("error opening output file:$!\n");
print TITLE "
\\rhead{\\thepage\\ of \\pageref{LastPage}}
\\chead[
\\large $cmd ($mon/$mday/$year)
]
{
\\large $cmd ($mon/$mday/$year)
}
\\cfoot[
\\scriptsize{$cmdline}
]
{
\\scriptsize{$cmdline}
}
";
close TITLE;

open(TABLES, ">job-table.tex") || die("error opening output file:$!\n");
print TABLES "
\\begin{tabular}{|p{.63\\columnwidth}|p{.63\\columnwidth}|p{.63\\columnwidth}|}
\\hline
uid: $uid \& nprocs: $nprocs \& runtime: $runtime seconds\\\\
\\hline
\\end{tabular}
";
close TABLES;

open(TABLES, ">access-table.tex") || die("error opening output file:$!\n");
print TABLES "
\\begin{tabular}{|r|r|}
\\multicolumn{2}{c}{Top 4 Access Sizes} \\\\
\\hline
access size \& count \\\\
\\hline
  $common{$max_access_hash}{CP_ACCESS1_ACCESS} \& $common{$max_access_hash}{CP_ACCESS1_COUNT} \\\\
  $common{$max_access_hash}{CP_ACCESS2_ACCESS} \& $common{$max_access_hash}{CP_ACCESS2_COUNT} \\\\
  $common{$max_access_hash}{CP_ACCESS3_ACCESS} \& $common{$max_access_hash}{CP_ACCESS3_COUNT} \\\\
  $common{$max_access_hash}{CP_ACCESS4_ACCESS} \& $common{$max_access_hash}{CP_ACCESS4_COUNT} \\\\
\\hline
\\end{tabular}
";
close TABLES;

open(TABLES, ">stride-table.tex") || die("error opening output file:$!\n");
print TABLES "
\\begin{tabular}{|r|r|}
\\multicolumn{2}{c}{file: $common{$max_access_hash}{'name'}} \\\\
\\hline
stride size \& count \\\\
\\hline
  $common{$max_access_hash}{CP_STRIDE1_STRIDE} \& $common{$max_access_hash}{CP_STRIDE1_COUNT} \\\\
  $common{$max_access_hash}{CP_STRIDE2_STRIDE} \& $common{$max_access_hash}{CP_STRIDE2_COUNT} \\\\
  $common{$max_access_hash}{CP_STRIDE3_STRIDE} \& $common{$max_access_hash}{CP_STRIDE3_COUNT} \\\\
  $common{$max_access_hash}{CP_STRIDE4_STRIDE} \& $common{$max_access_hash}{CP_STRIDE4_COUNT} \\\\
\\hline
\\end{tabular}
";
close TABLES;


open(TIME, ">time-summary.dat") || die("error opening output file:$!\n");
print TIME "# <type>, <app time>, <read>, <write>, <meta>\n";
print TIME "POSIX, ", ((($runtime * $nprocs - $summary{CP_F_POSIX_READ_TIME} -
    $summary{CP_F_POSIX_WRITE_TIME} -
    $summary{CP_F_POSIX_META_TIME})/($runtime * $nprocs)) * 100);
print TIME ", ", (($summary{CP_F_POSIX_READ_TIME}/($runtime * $nprocs))*100);
print TIME ", ", (($summary{CP_F_POSIX_WRITE_TIME}/($runtime * $nprocs))*100);
print TIME ", ", (($summary{CP_F_POSIX_META_TIME}/($runtime * $nprocs))*100), "\n";
print TIME "MPI-IO, ", ((($runtime * $nprocs - $summary{CP_F_MPI_READ_TIME} -
    $summary{CP_F_MPI_WRITE_TIME} -
    $summary{CP_F_MPI_META_TIME})/($runtime * $nprocs)) * 100);
print TIME ", ", (($summary{CP_F_MPI_READ_TIME}/($runtime * $nprocs))*100);
print TIME ", ", (($summary{CP_F_MPI_WRITE_TIME}/($runtime * $nprocs))*100);
print TIME ", ", (($summary{CP_F_MPI_META_TIME}/($runtime * $nprocs))*100), "\n";
close TIME;

# execute gnuplot scripts
system "gnuplot counts-pdf.gplt";
system "gnuplot hist-pdf.gplt";
system "gnuplot pattern-pdf.gplt";
system "gnuplot align-pdf.gplt";
system "gnuplot iodist-pdf.gplt";
system "gnuplot types-pdf.gplt";
system "gnuplot time-summary-pdf.gplt";

# generate summary PDF
system "pdflatex -halt-on-error summary.tex > latex.output";
system "pdflatex -halt-on-error summary.tex > latex.output2";

