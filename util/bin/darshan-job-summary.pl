#!/usr/bin/perl -w
#
#  (C) 2009 by Argonne National Laboratory.
#      See COPYRIGHT in top-level directory.
#

use FindBin;
use lib "$FindBin::Bin/../lib/";
use TeX::Encode;
use Encode;
use File::Temp qw/ tempdir /;
use Cwd;
use Getopt::Long;
use English;

my $gnuplot = "";

my $tmp_dir = tempdir( CLEANUP => 1 );
#my $tmp_dir = tempdir( CLEANUP => 0 );
#print "tmp dir: $tmp_dir\n";

my $orig_dir = getcwd;
my $output_file = "summary.pdf";
my $input_file = "";
my %access_hash = ();
my @access_size = ();

process_args();

open(TRACE, $input_file) || die("can't open $input_file for processing: $!\n");
open(FA_READ, ">$tmp_dir/file-access-read.dat") || die("error opening output file: $!\n");
open(FA_WRITE, ">$tmp_dir/file-access-write.dat") || die("error opening output file: $!\n");
open(FA_READ_SH, ">$tmp_dir/file-access-read-sh.dat") || die("error opening output file: $!\n");
open(FA_WRITE_SH, ">$tmp_dir/file-access-write-sh.dat") || die("error opening output file: $!\n");

my $last_read_start = 0;
my $last_write_start = 0;

my $cumul_read_indep = 0;
my $cumul_read_duration_indep = 0;
my $cumul_read_bytes_indep = 0;

my $cumul_write_indep = 0;
my $cumul_write_duration_indep = 0;
my $cumul_write_bytes_indep = 0;

my $cumul_read_shared = 0;
my $cumul_read_duration_shared = 0;
my $cumul_read_bytes_shared = 0;

my $cumul_write_shared = 0;
my $cumul_write_duration_shared = 0;
my $cumul_write_bytes_shared = 0;

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

        # seperate accumulators for independent and shared reads and writes
        if ($fields[2] eq "CP_F_POSIX_READ_TIME" && $fields[1] == -1){
            $cumul_read_shared += $fields[3];
        }
        if ($fields[2] eq "CP_F_POSIX_READ_TIME" && $fields[1] != -1){
            $cumul_read_indep += $fields[3];
        }
        if ($fields[2] eq "CP_F_POSIX_WRITE_TIME" && $fields[1] == -1){
            $cumul_write_shared += $fields[3];
        }
        if ($fields[2] eq "CP_F_POSIX_WRITE_TIME" && $fields[1] != -1){
            $cumul_write_indep += $fields[3];
        }

        if ($fields[2] eq "CP_BYTES_READ" && $fields[1] == -1){
            $cumul_read_bytes_shared += $fields[3];
        }
        if ($fields[2] eq "CP_BYTES_READ" && $fields[1] != -1){
            $cumul_read_bytes_indep += $fields[3];
        }
        if ($fields[2] eq "CP_BYTES_WRITTEN" && $fields[1] == -1){
            $cumul_write_bytes_shared += $fields[3];
        }
        if ($fields[2] eq "CP_BYTES_WRITTEN" && $fields[1] != -1){
            $cumul_write_bytes_indep += $fields[3];
        }

        # record start and end of reads and writes

        if ($fields[2] eq "CP_F_READ_START_TIMESTAMP") {
            # store until we find the end
            # adjust for systems that give absolute time stamps
            $last_read_start = $fields[3];
        }
        if ($fields[2] eq "CP_F_READ_END_TIMESTAMP" && $fields[3] != 0) {
            # assume we got the read start already 
            my $xdelta = $fields[3] - $last_read_start;
            # adjust for systems that have absolute time stamps 
            if($last_read_start > $starttime) {
                $last_read_start -= $starttime;
            }
            if($fields[0] == -1){
                $cumul_read_duration_shared += $xdelta;
                print FA_READ_SH "$last_read_start\t0\t$xdelta\t0\n";
            }
            else{
                $cumul_read_duration_indep += $xdelta;
                print FA_READ "$last_read_start\t$fields[0]\t$xdelta\t0\n";
            }
        }
        if ($fields[2] eq "CP_F_WRITE_START_TIMESTAMP") {
            # store until we find the end
            $last_write_start = $fields[3];
        }
        if ($fields[2] eq "CP_F_WRITE_END_TIMESTAMP" && $fields[3] != 0) {
            # assume we got the write start already 
            my $xdelta = $fields[3] - $last_write_start;
            # adjust for systems that have absolute time stamps 
            if($last_write_start > $starttime) {
                $last_write_start -= $starttime;
            }
            if($fields[0] == -1){
                $cumul_write_duration_shared += $xdelta;
                print FA_WRITE_SH "$last_write_start\t0\t$xdelta\t0\n";
            }
            else{
                $cumul_write_duration_indep += $xdelta;
                print FA_WRITE "$last_write_start\t$fields[0]\t$xdelta\t0\n";
            }
        }

        if ($fields[2] =~ /^CP_ACCESS(.)_ACCESS/) {
            $access_size[$1] = $fields[3];
        }
        if ($fields[2] =~ /^CP_ACCESS(.)_COUNT/) {
            my $tmp_access_size = $access_size[$1];
            if(defined $access_hash{$tmp_access_size}){
                $access_hash{$tmp_access_size} += $fields[3];
            }
            else{
                $access_hash{$tmp_access_size} = $fields[3];
            }
        }
    }
}

# Fudge one point at the end to make xrange match in read and write plots.
# For some reason I can't get the xrange command to work.  -Phil
print FA_READ "$runtime\t-1\t0\t0\n";
print FA_WRITE "$runtime\t-1\t0\t0\n";
print FA_READ_SH "$runtime\t0\t0\t0\n";
print FA_WRITE_SH "$runtime\t0\t0\t0\n";
close(FA_READ);
close(FA_WRITE);
close(FA_READ_SH);
close(FA_WRITE_SH);

# counts of operations
open(COUNTS, ">$tmp_dir/counts.dat") || die("error opening output file: $!\n");
print COUNTS "# P=POSIX, MI=MPI-IO indep., MC=MPI-IO coll., R=read, W=write\n";
print COUNTS "# PR, MIR, MCR, PW, MIW, MCW, Popen, Pseek, Pstat\n";
my $total_syncs = $summary{CP_POSIX_FSYNCS} + $summary{CP_POSIX_FDSYNCS};
print COUNTS "Read, ", $summary{CP_POSIX_READS}, ", ",
    $summary{CP_INDEP_READS}, ", ", $summary{CP_COLL_READS}, "\n",
    "Write, ", $summary{CP_POSIX_WRITES}, ", ", 
    $summary{CP_INDEP_WRITES}, ", ", $summary{CP_COLL_WRITES}, "\n",
    "Open, ", $summary{CP_POSIX_OPENS}, ", ", $summary{CP_INDEP_OPENS},", ",
    $summary{CP_COLL_OPENS}, "\n",
    "Stat, ", $summary{CP_POSIX_STATS}, ", 0, 0\n",
    "Seek, ", $summary{CP_POSIX_SEEKS}, ", 0, 0\n",
    "Mmap, ", $summary{CP_POSIX_MMAPS}, ", 0, 0\n",
    "Fsync, ", $total_syncs, ", 0, 0\n";
close COUNTS;

# histograms of reads and writes
open (HIST, ">$tmp_dir/hist.dat") || die("error opening output file: $!\n");
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
open (PATTERN, ">$tmp_dir/pattern.dat") || die("error opening output file: $!\n");
print PATTERN "# op total sequential consecutive\n";
print PATTERN "Read, ", $summary{CP_POSIX_READS}, ", ",
    $summary{CP_SEQ_READS}, ", ", $summary{CP_CONSEC_READS}, "\n";
print PATTERN "Write, ", $summary{CP_POSIX_WRITES}, ", ",
    $summary{CP_SEQ_WRITES}, ", ", $summary{CP_CONSEC_WRITES}, "\n";
close PATTERN;

# aligned I/O
open (ALIGN, ">$tmp_dir/align.dat") || die("error opening output file: $!\n");
print ALIGN "# total unaligned_mem unaligned_file align_mem align_file\n";
print ALIGN $summary{CP_POSIX_READS} + $summary{CP_POSIX_WRITES}, ", ",
    $summary{CP_MEM_NOT_ALIGNED}, ", ", $summary{CP_FILE_NOT_ALIGNED}, "\n";
close ALIGN;

# MPI types
open (TYPES, ">$tmp_dir/types.dat") || die("error opening output file: $!\n");
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

open(IODIST, ">$tmp_dir/iodist.dat") || die("error opening output file: $!\n");
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

open(TITLE, ">$tmp_dir/title.tex") || die("error opening output file:$!\n");
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

open(TABLES, ">$tmp_dir/job-table.tex") || die("error opening output file:$!\n");
print TABLES "
\\begin{tabular}{|p{.63\\columnwidth}|p{.63\\columnwidth}|p{.63\\columnwidth}|}
\\hline
uid: $uid \& nprocs: $nprocs \& runtime: $runtime seconds\\\\
\\hline
\\end{tabular}
";
close TABLES;

open(TABLES, ">$tmp_dir/access-table.tex") || die("error opening output file:$!\n");
print TABLES "
\\begin{tabular}{|r|r|}
\\multicolumn{2}{c}{Most Common Access Sizes} \\\\
\\hline
access size \& count \\\\
\\hline
";

# sort access sizes (descending)
my $i = 0;
foreach $value (sort {$access_hash{$b} <=> $access_hash{$a} } keys %access_hash) {
    if($i == 4) {
        last;
    }
    if($access_hash{$value} == 0) {
        last;
    }
    print TABLES "$value \& $access_hash{$value} \\\\\n";
    $i++;
}

print TABLES "
\\hline
\\end{tabular}
";
close TABLES;

open(TIME, ">$tmp_dir/time-summary.dat") || die("error opening output file:$!\n");
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

# copy template files to tmp tmp_dir
system "cp $FindBin::Bin/../share/*.gplt $tmp_dir/";
system "cp $FindBin::Bin/../share/*.tex $tmp_dir/";

# generate template for file access plot (we have to set range)
my $ymax = $nprocs + 1;
open(FILEACC, ">$tmp_dir/file-access-read-eps.gplt") || die("error opening output file:$!\n");
print FILEACC "#!/usr/bin/gnuplot -persist

set terminal postscript eps color solid font \"Helvetica\" 18 size 10in,2.5in
set output \"file-access-read.eps\"
set ylabel \"MPI rank\"
set xlabel \"hours:minutes:seconds\"
set xdata time
set timefmt \"%s\"
set format x \"%H:%M:%S\"
set yrange [-1:$ymax]
set title \"Duration from first to last read access on independent files\"
# the xrange doesn't work for some reason
#set xrange [0:$runtime]
#set ytics -1,1
set lmargin 5

# color blindness work around
set style line 2 lc 3
set style line 3 lc 4
set style line 4 lc 5
set style line 5 lc 2
set style increment user

# lw 3 to make lines thicker...
# note that writes are slightly offset for better visibility
plot \"file-access-read.dat\" using 1:2:3:4 with vectors nohead filled notitle
";
close FILEACC;

open(FILEACC, ">$tmp_dir/file-access-write-eps.gplt") || die("error opening output file:$!\n");
print FILEACC "#!/usr/bin/gnuplot -persist

set terminal postscript eps color solid font \"Helvetica\" 18 size 10in,2.5in
set output \"file-access-write.eps\"
set ylabel \"MPI rank\"
set xlabel \"hours:minutes:seconds\"
set xdata time
set timefmt \"%s\"
set format x \"%H:%M:%S\"
set title \"Duration from first to last write access on independent files\"
set yrange [-1:$ymax]
# the xrange doesn't work for some reason
# set xrange [0:$runtime]
#set ytics -1,1
set lmargin 5

# color blindness work around
set style line 2 lc 3
set style line 3 lc 4
set style line 4 lc 5
set style line 5 lc 2
set style increment user

# lw 3 to make lines thicker...
plot \"file-access-write.dat\" using 1:2:3:4 with vectors nohead filled lt 2 notitle
";
close FILEACC;

open(FILEACC, ">$tmp_dir/file-access-shared-eps.gplt") || die("error opening output file:$!\n");
print FILEACC "#!/usr/bin/gnuplot -persist

set terminal postscript eps color solid font \"Helvetica\" 18 size 10in,2.5in
set output \"file-access-shared.eps\"
set xlabel \"hours:minutes:seconds\"
set xdata time
set timefmt \"%s\"
set format x \"%H:%M:%S\"
unset ytics
set ylabel \"All processes\"
# the xrange doesn't work for some reason
# set xrange [0:$runtime]
set yrange [-1:1]
set title \"Duration from first to last access on shared files\"
set lmargin 5

# color blindness work around
set style line 2 lc 3
set style line 3 lc 4
set style line 4 lc 5
set style line 5 lc 2
set style increment user

plot \"file-access-read-sh.dat\" using 1:2:3:4 with vectors nohead filled lw 10 title \"read\", \\
\"file-access-write-sh.dat\" using 1:((\$2)-.2):3:4 with vectors nohead filled lw 10 title \"write\"
";
close FILEACC;


if(-x "$FindBin::Bin/gnuplot")
{
    $gnuplot = "$FindBin::Bin/gnuplot";
}
else
{
    $gnuplot = "gnuplot";
}

# move to tmp_dir
chdir $tmp_dir;

# execute gnuplot scripts
system "$gnuplot counts-eps.gplt";
system "epstopdf counts.eps";
system "$gnuplot hist-eps.gplt";
system "epstopdf hist.eps";
system "$gnuplot pattern-eps.gplt";
system "epstopdf pattern.eps";
system "$gnuplot time-summary-eps.gplt";
system "epstopdf time-summary.eps";
system "$gnuplot file-access-read-eps.gplt";
system "epstopdf file-access-read.eps";
system "$gnuplot file-access-write-eps.gplt";
system "epstopdf file-access-write.eps";
system "$gnuplot file-access-shared-eps.gplt";
system "epstopdf file-access-shared.eps";

#system "gnuplot align-pdf.gplt";
#system "gnuplot iodist-pdf.gplt";
#system "gnuplot types-pdf.gplt";

# generate summary PDF
system "pdflatex -halt-on-error summary.tex > latex.output";
system "pdflatex -halt-on-error summary.tex > latex.output2";

# get back out of tmp dir and grab results
chdir $orig_dir;
system "mv $tmp_dir/summary.pdf $output_file";

sub process_args
{
    use vars qw( $opt_help $opt_output );

    Getopt::Long::Configure("no_ignore_case", "bundling");
    GetOptions( "help",
        "output=s");

    if($opt_help)
    {
        print_help();
        exit(0);
    }

    if($opt_output)
    {
        $output_file = $opt_output;
    }

    # there should only be one remaining argument: the input file 
    if($#ARGV != 0)
    {
        print "Error: invalid arguments.\n";
        print_help();
        exit(1);
    }
    $input_file = $ARGV[0];

    return;
}

sub print_help
{
    print <<EOF;

Usage: $PROGRAM_NAME <options> input_file

    --help          Prints this help message
    --output        Specifies a file to write pdf output to
                    (defaults to ./summary.pdf)
Purpose:

    This script reads a Darshan output file in text format (as 
    produced by the darshan-parser utility) and generates a pdf file 
    summarizing job behavior.

EOF
    return;
}
