#!/usr/bin/perl -w
#
#  (C) 2009 by Argonne National Laboratory.
#      See COPYRIGHT in top-level directory.
#

use lib "/home/pcarns/working/darshan/install/lib";
use TeX::Encode;
use Encode;
use File::Temp qw/ tempdir /;
use Cwd;
use Getopt::Long;
use English;
use POSIX qw(strftime);

#
# system commands used
#
my $darshan_parser = "darshan-parser";
my $pdflatex       = "pdflatex";
my $epstopdf       = "epstopdf";
my $cp             = "cp";
my $mv             = "mv";
my $gnuplot        ="gnuplot";

my $orig_dir = getcwd;
my $output_file = "summary.pdf";
my $verbose_flag = 0;
my $input_file = "";
my %access_hash = ();
my @access_size = ();
my %hash_files = ();
my $jobid = 0;

# data structures for calculating performance
my %hash_unique_file_time = ();
my $shared_file_time = 0;
my $total_job_bytes = 0;

process_args();

#grab jobid from name, old logs don't store it in the file
if($input_file =~ /_id(\d+)_/) {
    $jobid = $1;
}

open(TRACE, "$darshan_parser $input_file |") || die("Can't execute \"$darshan_parser $input_file\": $!\n");


my $last_read_start = 0;
my $last_write_start = 0;

my $cumul_read_indep = 0;
my $cumul_read_bytes_indep = 0;

my $cumul_write_indep = 0;
my $cumul_write_bytes_indep = 0;

my $cumul_read_shared = 0;
my $cumul_read_bytes_shared = 0;

my $cumul_write_shared = 0;
my $cumul_write_bytes_shared = 0;

my $cumul_meta_shared = 0;
my $cumul_meta_indep = 0;

my $first_data_line = 1;
my $current_rank = 0;
my $current_hash = 0;
my %file_record_hash = ();

my %fs_data = ();

while ($line = <TRACE>) {
    chomp($line);
    
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
        if ($line =~ /^# darshan log version: /) {
            ($junk, $version) = split(':', $line, 2);
            $version =~ s/^\s+//;
        }
    }
    else {
        # parse line
	@fields = split(/[\t ]+/, $line);

        # is this our first piece of data?
        if($first_data_line)
        {
            $current_rank = $fields[0];
            $current_hash = $fields[1];
            $first_data_line = 0;
        }

        # is this a new file record?
        if($fields[0] != $current_rank || $fields[1] != $current_hash)
        {
            $file_record_hash{CP_NAME_SUFFIX} = $fields[4];

            # process previous record
            process_file_record($current_rank, $current_hash, \%file_record_hash);

            # reset variables for next record 
            $current_rank = $fields[0];
            $current_hash = $fields[1];
            %file_record_hash = ();
        }

        $file_record_hash{$fields[2]} = $fields[3];


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
        if ($fields[2] eq "CP_F_POSIX_READ_TIME" && $fields[0] == -1){
            $cumul_read_shared += $fields[3];
        }
        if ($fields[2] eq "CP_F_POSIX_READ_TIME" && $fields[0] != -1){
            $cumul_read_indep += $fields[3];
        }
        if ($fields[2] eq "CP_F_POSIX_WRITE_TIME" && $fields[0] == -1){
            $cumul_write_shared += $fields[3];
        }
        if ($fields[2] eq "CP_F_POSIX_WRITE_TIME" && $fields[0] != -1){
            $cumul_write_indep += $fields[3];
        }

        if ($fields[2] eq "CP_F_POSIX_META_TIME" && $fields[0] == -1){
            $cumul_meta_shared += $fields[3];
        }
        if ($fields[2] eq "CP_F_POSIX_META_TIME" && $fields[0] != -1){
            $cumul_meta_indep += $fields[3];
        }

        if ((($fields[2] eq "CP_BYTES_READ") or
             ($fields[2] eq "CP_BYTES_WRITTEN")) and
            not defined($fs_data{$fields[5]}))
        {
            $fs_data{$fields[5]} = [0,0];
        }

        if ($fields[2] eq "CP_BYTES_READ" && $fields[0] == -1){
            $cumul_read_bytes_shared += $fields[3];
            $fs_data{$fields[5]}->[0] += $fields[3];
        }
        if ($fields[2] eq "CP_BYTES_READ" && $fields[0] != -1){
            $cumul_read_bytes_indep += $fields[3];
            $fs_data{$fields[5]}->[0] += $fields[3];
        }
        if ($fields[2] eq "CP_BYTES_WRITTEN" && $fields[0] == -1){
            $cumul_write_bytes_shared += $fields[3];
            $fs_data{$fields[5]}->[1] += $fields[3];
        }
        if ($fields[2] eq "CP_BYTES_WRITTEN" && $fields[0] != -1){
            $cumul_write_bytes_indep += $fields[3];
            $fs_data{$fields[5]}->[1] += $fields[3];
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
            }
            else{
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
            }
            else{
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

#
# Exit out if there are no actual file accesses
#
if ($first_data_line)
{
    $strtm = strftime("%a %b %e %H:%M:%S %Y", localtime($starttime));

    print "This darshan log has no file records. No summary was produced.\n";
    print "    jobid:$jobid\n";
    print "      uid:$uid\n";
    print "starttime: $strtm ($starttime )\n";
    print "  runtime:$runtime (seconds)\n";
    print "   nprocs:$nprocs\n";
    print "  version: $version\n";
    close(TRACE);
    exit(1);
}

# process last file record
$file_record_hash{CP_NAME_SUFFIX} = $fields[4];
process_file_record($current_rank, $current_hash, \%file_record_hash);
close(TRACE) || die "darshan-parser failure: $! $?";

# Fudge one point at the end to make xrange match in read and write plots.
# For some reason I can't get the xrange command to work.  -Phil
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

# generate title for summary

my $counter;
my $sum;
my $max;
my $key;
my $avg;

$counter = 0;
$sum = 0;
$max = 0;
foreach $key (keys %hash_files) {
    $counter++;
    if($hash_files{$key}{'min_open_size'} >
        $hash_files{$key}{'max_size'})
    {
        $sum += $hash_files{$key}{'min_open_size'};
        if($hash_files{$key}{'min_open_size'} > $max)
        {
            $max = $hash_files{$key}{'min_open_size'};
        }
    }
    else
    {
        $sum += $hash_files{$key}{'max_size'};
        if($hash_files{$key}{'max_size'} > $max)
        {
            $max = $hash_files{$key}{'max_size'};
        }
    }
}
if($counter > 0) { $avg = $sum / $counter; }
else { $avg = 0; }

print "$jobid\t$counter\t$avg\t$max\t";

$counter = 0;
$sum = 0;
$max = 0;
foreach $key (keys %hash_files) {
    if($hash_files{$key}{'was_read'} && !($hash_files{$key}{'was_written'}))
    {
        $counter++;
        if($hash_files{$key}{'min_open_size'} >
            $hash_files{$key}{'max_size'})
        {
            $sum += $hash_files{$key}{'min_open_size'};
            if($hash_files{$key}{'min_open_size'} > $max)
            {
                $max = $hash_files{$key}{'min_open_size'};
            }
        }
        else
        {
            $sum += $hash_files{$key}{'max_size'};
            if($hash_files{$key}{'max_size'} > $max)
            {
                $max = $hash_files{$key}{'max_size'};
            }
        }
    }
}
if($counter > 0) { $avg = $sum / $counter; }
else { $avg = 0; }
print "$counter\t$avg\t$max\t";

$counter = 0;
$sum = 0;
$max = 0;
foreach $key (keys %hash_files) {
    if(!($hash_files{$key}{'was_read'}) && $hash_files{$key}{'was_written'})
    {
        $counter++;
        if($hash_files{$key}{'min_open_size'} >
            $hash_files{$key}{'max_size'})
        {
            $sum += $hash_files{$key}{'min_open_size'};
            if($hash_files{$key}{'min_open_size'} > $max)
            {
                $max = $hash_files{$key}{'min_open_size'};
            }
        }
        else
        {
            $sum += $hash_files{$key}{'max_size'};
            if($hash_files{$key}{'max_size'} > $max)
            {
                $max = $hash_files{$key}{'max_size'};
            }
        }
    }
}
if($counter > 0) { $avg = $sum / $counter; }
else { $avg = 0; }
print "$counter\t$avg\t$max\t";

$counter = 0;
$sum = 0;
$max = 0;
foreach $key (keys %hash_files) {
    if($hash_files{$key}{'was_read'} && $hash_files{$key}{'was_written'})
    {
        $counter++;
        if($hash_files{$key}{'min_open_size'} >
            $hash_files{$key}{'max_size'})
        {
            $sum += $hash_files{$key}{'min_open_size'};
            if($hash_files{$key}{'min_open_size'} > $max)
            {
                $max = $hash_files{$key}{'min_open_size'};
            }
        }
        else
        {
            $sum += $hash_files{$key}{'max_size'};
            if($hash_files{$key}{'max_size'} > $max)
            {
                $max = $hash_files{$key}{'max_size'};
            }
        }
    }
}
if($counter > 0) { $avg = $sum / $counter; }
else { $avg = 0; }
print "$counter\t$avg\t$max\t";

$counter = 0;
$sum = 0;
$max = 0;
foreach $key (keys %hash_files) {
    if($hash_files{$key}{'was_written'} &&
        $hash_files{$key}{'min_open_size'} == 0 &&
        $hash_files{$key}{'max_size'} > 0)
    {
        $counter++;
        if($hash_files{$key}{'min_open_size'} >
            $hash_files{$key}{'max_size'})
        {
            $sum += $hash_files{$key}{'min_open_size'};
            if($hash_files{$key}{'min_open_size'} > $max)
            {
                $max = $hash_files{$key}{'min_open_size'};
            }
        }
        else
        {
            $sum += $hash_files{$key}{'max_size'};
            if($hash_files{$key}{'max_size'} > $max)
            {
                $max = $hash_files{$key}{'max_size'};
            }
        }
    }
}
if($counter > 0) { $avg = $sum / $counter; }
else { $avg = 0; }
print "$counter\t$avg\t$max\n";

$cumul_read_indep /= $nprocs;
$cumul_read_bytes_indep /= $nprocs;
$cumul_read_bytes_indep /= 1048576.0;

$cumul_write_indep /= $nprocs;
$cumul_write_bytes_indep /= $nprocs;
$cumul_write_bytes_indep /= 1048576.0;

$cumul_read_shared /= $nprocs;
$cumul_read_bytes_shared /= $nprocs;
$cumul_read_bytes_shared /= 1048576.0;

$cumul_write_shared /= $nprocs;
$cumul_write_bytes_shared /= $nprocs;
$cumul_write_bytes_shared /= 1048576.0;

$cumul_meta_shared /= $nprocs;
$cumul_meta_indep /= $nprocs;



sub process_file_record
{
    my $rank = $_[0];
    my $hash = $_[1];
    my(%file_record) = %{$_[2]};

    if($file_record{'CP_INDEP_OPENS'} == 0 &&
        $file_record{'CP_COLL_OPENS'} == 0 &&
        $file_record{'CP_POSIX_OPENS'} == 0 &&
        $file_record{'CP_POSIX_FOPENS'} == 0)
    {
        # file wasn't really opened, just stat probably
        return;
    }

    # record smallest open time size reported by any rank
    if(!defined($hash_files{$hash}{'min_open_size'}) ||
        $hash_files{$hash}{'min_open_size'} > 
        $file_record{'CP_SIZE_AT_OPEN'})
    {
        $hash_files{$hash}{'min_open_size'} = 
            $file_record{'CP_SIZE_AT_OPEN'};
    }

    # record largest size that the file reached at any rank
    if(!defined($hash_files{$hash}{'max_size'}) ||
        $hash_files{$hash}{'max_size'} <  
        ($file_record{'CP_MAX_BYTE_READ'} + 1))
    {
        $hash_files{$hash}{'max_size'} = 
            $file_record{'CP_MAX_BYTE_READ'} + 1;
    }
    if(!defined($hash_files{$hash}{'max_size'}) ||
        $hash_files{$hash}{'max_size'} <  
        ($file_record{'CP_MAX_BYTE_WRITTEN'} + 1))
    {
        $hash_files{$hash}{'max_size'} = 
            $file_record{'CP_MAX_BYTE_WRITTEN'} + 1;
    }

    # make sure there is an initial value for read and write flags
    if(!defined($hash_files{$hash}{'was_read'}))
    {
        $hash_files{$hash}{'was_read'} = 0;
    }
    if(!defined($hash_files{$hash}{'was_written'}))
    {
        $hash_files{$hash}{'was_written'} = 0;
    }

    if($file_record{'CP_INDEP_OPENS'} > 0 ||
        $file_record{'CP_COLL_OPENS'} > 0)
    {
        # mpi file
        if($file_record{'CP_INDEP_READS'} > 0 ||
            $file_record{'CP_COLL_READS'} > 0 ||
            $file_record{'CP_SPLIT_READS'} > 0 ||
            $file_record{'CP_NB_READS'} > 0)
        {
            # data was read from the file
            $hash_files{$hash}{'was_read'} = 1;
        }
        if($file_record{'CP_INDEP_WRITES'} > 0 ||
            $file_record{'CP_COLL_WRITES'} > 0 ||
            $file_record{'CP_SPLIT_WRITES'} > 0 ||
            $file_record{'CP_NB_WRITES'} > 0)
        {
            # data was written to the file
            $hash_files{$hash}{'was_written'} = 1;
        }
    }
    else
    {
        # posix file
        if($file_record{'CP_POSIX_READS'} > 0 ||
            $file_record{'CP_POSIX_FREADS'} > 0)
        {
            # data was read from the file
            $hash_files{$hash}{'was_read'} = 1;
        }
        if($file_record{'CP_POSIX_WRITES'} > 0 ||
            $file_record{'CP_POSIX_FWRITES'} > 0)
        {
            # data was written to the file 
            $hash_files{$hash}{'was_written'} = 1;
        }
    }

    $hash_files{$hash}{'name'} = $file_record{CP_NAME_SUFFIX};

    if ($rank == -1)
    {
        $hash_files{$hash}{'procs'}          = $nprocs;
        $hash_files{$hash}{'slowest_rank'}   = $file_record{'CP_SLOWEST_RANK'};
        $hash_files{$hash}{'slowest_time'}   = $file_record{'CP_F_SLOWEST_RANK_TIME'};
        $hash_files{$hash}{'slowest_bytes'}  = $file_record{'CP_SLOWEST_RANK_BYTES'};
        $hash_files{$hash}{'fastest_rank'}   = $file_record{'CP_FASTEST_RANK'};
        $hash_files{$hash}{'fastest_time'}   = $file_record{'CP_F_FASTEST_RANK_TIME'};
        $hash_files{$hash}{'fastest_bytes'}  = $file_record{'CP_FASTEST_RANK_BYTES'};
        $hash_files{$hash}{'variance_time'}  = $file_record{'CP_F_VARIANCE_RANK_TIME'};
        $hash_files{$hash}{'variance_bytes'} = $file_record{'CP_F_VARIANCE_RANK_BYTES'};
    }
    else
    {
        my $total_time = $file_record{'CP_F_POSIX_META_TIME'} +
                         $file_record{'CP_F_POSIX_READ_TIME'} +
                         $file_record{'CP_F_POSIX_WRITE_TIME'};

        my $total_bytes = $file_record{'CP_BYTES_READ'} +
                          $file_record{'CP_BYTES_WRITTEN'};

        if(!defined($hash_files{$hash}{'slowest_time'}) ||
           $hash_files{$hash}{'slowest_time'} < $total_time)
        {
            $hash_files{$hash}{'slowest_time'}  = $total_time;
            $hash_files{$hash}{'slowest_rank'}  = $rank;
            $hash_files{$hash}{'slowest_bytes'} = $total_bytes;
        }

        if(!defined($hash_files{$hash}{'fastest_time'}) ||
           $hash_files{$hash}{'fastest_time'} > $total_time)
        {
            $hash_files{$hash}{'fastest_time'}  = $total_time;
            $hash_files{$hash}{'fastest_rank'}  = $rank;
            $hash_files{$hash}{'fastest_bytes'} = $total_bytes;
        }

        if(!defined($hash_files{$hash}{'variance_time_S'}))
        {
            $hash_files{$hash}{'variance_time_S'} = 0;
            $hash_files{$hash}{'variance_time_T'} = $total_time;
            $hash_files{$hash}{'variance_time_n'} = 1;
            $hash_files{$hash}{'variance_bytes_S'} = 0;
            $hash_files{$hash}{'variance_bytes_T'} = $total_bytes;
            $hash_files{$hash}{'variance_bytes_n'} = 1;
            $hash_files{$hash}{'procs'} = 1;
            $hash_files{$hash}{'variance_time'} = 0;
            $hash_files{$hash}{'variance_bytes'} = 0;
        }
        else
        {
            my $n = $hash_files{$hash}{'variance_time_n'};
            my $m = 1;
            my $T = $hash_files{$hash}{'variance_time_T'};
            $hash_files{$hash}{'variance_time_S'} += ($m/($n*($n+$m)))*(($n/$m)*$total_time - $T)*(($n/$m)*$total_time - $T);
            $hash_files{$hash}{'variance_time_T'} += $total_time;
            $hash_files{$hash}{'variance_time_n'} += 1;

            $hash_files{$hash}{'variance_time'}    = $hash_files{$hash}{'variance_time_S'} / $hash_files{$hash}{'variance_time_n'};

            $n = $hash_files{$hash}{'variance_bytes_n'};
            $m = 1;
            $T = $hash_files{$hash}{'variance_bytes_T'};
            $hash_files{$hash}{'variance_bytes_S'} += ($m/($n*($n+$m)))*(($n/$m)*$total_bytes - $T)*(($n/$m)*$total_bytes - $T);
            $hash_files{$hash}{'variance_bytes_T'} += $total_bytes;
            $hash_files{$hash}{'variance_bytes_n'} += 1;

            $hash_files{$hash}{'variance_bytes'}    = $hash_files{$hash}{'variance_bytes_S'} / $hash_files{$hash}{'variance_bytes_n'};

            $hash_files{$hash}{'procs'} = $n;
        }
    }

    # if this is a non-shared file, then add the time spent here to the
    # total for that particular rank
    if ($rank != -1)
    {
        # is it mpi-io or posix?
        if($file_record{CP_INDEP_OPENS} > 0 ||
            $file_record{CP_COLL_OPENS} > 0)
        {
            # add up mpi times
            if(defined($hash_unique_file_time{$rank}))
            {
                $hash_unique_file_time{$rank} +=
                    $file_record{CP_F_MPI_META_TIME} + 
                    $file_record{CP_F_MPI_READ_TIME} + 
                    $file_record{CP_F_MPI_WRITE_TIME};
            }
            else
            {
                $hash_unique_file_time{$rank} =
                    $file_record{CP_F_MPI_META_TIME} + 
                    $file_record{CP_F_MPI_READ_TIME} + 
                    $file_record{CP_F_MPI_WRITE_TIME};
            }
        }
        else
        {
            # add up posix times
            if(defined($hash_unique_file_time{$rank}))
            {
                $hash_unique_file_time{$rank} +=
                    $file_record{CP_F_POSIX_META_TIME} + 
                    $file_record{CP_F_POSIX_READ_TIME} + 
                    $file_record{CP_F_POSIX_WRITE_TIME};
            }
            else
            {
                $hash_unique_file_time{$rank} =
                    $file_record{CP_F_POSIX_META_TIME} + 
                    $file_record{CP_F_POSIX_READ_TIME} + 
                    $file_record{CP_F_POSIX_WRITE_TIME};
            }
        }
    }

    my $mpi_did_read = 
        $file_record{'CP_INDEP_READS'} + 
        $file_record{'CP_COLL_READS'} + 
        $file_record{'CP_NB_READS'} + 
        $file_record{'CP_SPLIT_READS'};

    # add up how many bytes were transferred
    if(($file_record{CP_INDEP_OPENS} > 0 ||
        $file_record{CP_COLL_OPENS} > 0) && (!($mpi_did_read)))
    {
        # mpi file that was only written; disregard any read accesses that
        # may have been performed for sieving at the posix level
        $total_job_bytes += $file_record{'CP_BYTES_WRITTEN'}; 
    }
    else
    {
        # normal case
        $total_job_bytes += $file_record{'CP_BYTES_WRITTEN'} +
            $file_record{'CP_BYTES_READ'};
    }

    # TODO 
    # (detect mpi or posix and):
    # - sum meta time per rank for uniq files
    # - sum io time per rank for uniq files
    # - sum time from first open to last io for shared files
    # - sum meta time/nprocs for shared files
    # - sum io time/nprocs for shared files
    
    # TODO: ideas
    # graph time spent performing I/O per rank
    # for rank that spent the most time performing I/O:
    # - meta on ro files, meta on wo files, read time, write time
    # table with nfiles accessed, ro, wo, rw, created
}

sub process_args
{
    use vars qw( $opt_help $opt_output $opt_verbose );

    Getopt::Long::Configure("no_ignore_case", "bundling");
    GetOptions( "help",
        "output=s",
        "verbose");

    if($opt_help)
    {
        print_help();
        exit(0);
    }

    if($opt_output)
    {
        $output_file = $opt_output;
    }

    if($opt_verbose)
    {
        $verbose_flag = $opt_verbose;
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
    --verbose       Prints and retains tmpdir used for LaTeX output

Purpose:

    This script reads a Darshan output file generated by a job and
    generates a pdf file summarizing job behavior.

EOF
    return;
}
