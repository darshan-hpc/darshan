#!/usr/bin/perl -w

# This script will go through all of the darshan logs in a given
# subdirectory and produce a file summarizing each job (id, start unix,
# nprocs, bytes read, bytes written, perf estimate) and then for each job
# generate two text files listing the read and write intervals (start, end,
# nbytes)

use strict;
use File::Find;

sub wanted
{
    my $file = $_;
    my $line;
    my $version = 0.0;
    my $nprocs = 0;
    my $start = 0;
    my $end = 0;
    my $start_a = "";
    my $end_a = "";
    my $jobid = 0;
    my $bytes_r = 0;
    my $bytes_w = 0;
    my $perf = 0.0;
    my $read_interval = {};
    my $write_interval = {};
    my @read_interval_array = ();
    my @write_interval_array = ();
    my @read_merged = ();
    my @write_merged = ();
    my @fields = ();
    my $idle_after;
    my $duration;
    my $rbytes = 0;
    my $wbytes = 0;
    my $uid = 0;

    # only operate on darshan log files
    $file =~ /\.darshan\.gz$/ or return;    

    print("current file: $File::Find::name\n");

    # grab jobid from name, old logs don't store it in the file
    if($file =~ /_id(\d+)_/) {
        $jobid = $1;
    }

    if(!(open(PARSE, "darshan-parser $file |")))
    {
        print(STDERR "Failed to parse $File::Find::name\n");
        return;
    }

    while ($line = <PARSE>) {
 
        if ($line =~ /^\s*$/) {
            # ignore blank lines
        }
        elsif ($line =~ /^#/) {
            if($line =~ /nprocs: (\S+)/) {
                $nprocs = $1;
            }
            if($line =~ /start_time: (\S+)/) {
                $start = $1;
            }
            if($line =~ /end_time: (\S+)/) {
                $end = $1;
            }
            if($line =~ /start_time_asci: (.+)/) {
                $start_a = "$1";
            }
            if($line =~ /end_time_asci: (.+)/) {
                $end_a = "$1";
            }
            if($line =~ /uid: (\S+)/) {
                $uid = "$1";
            }
        }
        else {
            # parse line
            @fields = split(/[\t ]+/, $line);
	
            if ($fields[2] eq "CP_BYTES_READ") {
                $read_interval->{bytes} = $fields[3];
                #print("read interval bytes: $fields[3]\n");
            }
            if ($fields[2] eq "CP_BYTES_WRITTEN") {
                $write_interval->{bytes} = $fields[3];
                #print("write interval bytes: $fields[3]\n");
            }
            if ($fields[2] eq "CP_F_READ_START_TIMESTAMP") {
                $read_interval->{start} = $fields[3];
            }
            if ($fields[2] eq "CP_F_WRITE_START_TIMESTAMP") {
                $write_interval->{start} = $fields[3];
            }
            if ($fields[2] eq "CP_F_READ_END_TIMESTAMP") {
                $read_interval->{end} = $fields[3];
                if($read_interval->{bytes} > 0) {
                    #print("pushing read interval bytes $read_interval->{bytes}\n");
                    push(@read_interval_array, $read_interval);
                    $read_interval = {};
                }
            }
            if ($fields[2] eq "CP_F_WRITE_END_TIMESTAMP") {
                $write_interval->{end} = $fields[3];
                if($write_interval->{bytes} > 0) {
                    #print("pushing write interval bytes $write_interval->{bytes}\n");
                    push(@write_interval_array, $write_interval);
                    $write_interval = {};
                }
            }

        }

    }

    if(!(close(PARSE)))
    {
        print(STDERR "Failed to parse $File::Find::name\n");
        return;
    }

    @read_interval_array = sort { $a->{start} <=> $b->{start} } @read_interval_array;
    @write_interval_array = sort { $a->{start} <=> $b->{start} } @write_interval_array;

    my $i;
    if($#read_interval_array > -1){
        push(@read_merged, $read_interval_array[0]);
    }
    if($#read_interval_array > 0){
        for $i (1 .. $#read_interval_array)
        {
            if($read_interval_array[$i]->{start} <=
                $read_merged[$#read_merged]->{end})
            {
                $read_merged[$#read_merged]->{bytes} +=
                    $read_interval_array[$i]->{bytes};
                if($read_interval_array[$i]->{end} > $read_merged[$#read_merged]->{end}) {
                    $read_merged[$#read_merged]->{end} = 
                        $read_interval_array[$i]->{end};
                }
            }
            else
            {
                push(@read_merged, $read_interval_array[$i]);
            }
        }
    }

    if($#write_interval_array > -1){
        push(@write_merged, $write_interval_array[0]);
    }
    if($#write_interval_array > 0){
        for $i (1 .. $#write_interval_array)
        {
            if($write_interval_array[$i]->{start} <=
                $write_merged[$#write_merged]->{end})
            {
                $write_merged[$#write_merged]->{bytes} +=
                    $write_interval_array[$i]->{bytes};
                if($write_interval_array[$i]->{end} > $write_merged[$#write_merged]->{end}) {
                    $write_merged[$#write_merged]->{end} = 
                        $write_interval_array[$i]->{end};
                }
            }
            else
            {
                push(@write_merged, $write_interval_array[$i]);
            }
        }
    }

    if(!(open(WFILE, ">$jobid-$start-write-intervals.dat")))
    {
        print(STDERR "Failed to open $jobid-write-intervals.dat\n");
        return;
    }
    print(WFILE "#<bytes>\t<start>\t<end>\t<duration>\t<idle time after>\n");

    if($#write_merged > -1) {
        for $i (0 .. $#write_merged)
        {
            if($i == $#write_merged) {
                $idle_after = ($end-$start + 1)-$write_merged[$i]->{end};
            }
            else {
                $idle_after = $write_merged[$i+1]->{start} -
                    $write_merged[$i]->{end};
            }
            $wbytes += $write_merged[$i]->{bytes};
            $duration = $write_merged[$i]->{end} - $write_merged[$i]->{start};
            print(WFILE
            "$write_merged[$i]->{bytes}\t$write_merged[$i]->{start}\t$write_merged[$i]->{end}\t$duration\t$idle_after\n");
        }
    }

    close(WFILE);

    if(!(open(RFILE, ">$jobid-$start-read-intervals.dat")))
    {
        print(STDERR "Failed to open $jobid-read-intervals.dat\n");
        return;
    }
    print(RFILE "#<bytes>\t<start>\t<end>\t<duration>\t<idle time after>\n");

    if($#read_merged > -1) {
        for $i (0 .. $#read_merged)
        {
            if($i == $#read_merged) {
                $idle_after = ($end-$start + 1)-$read_merged[$i]->{end};
            }
            else {
                $idle_after = $read_merged[$i+1]->{start} -
                    $read_merged[$i]->{end};
            }
            $rbytes += $read_merged[$i]->{bytes};
            $duration = $read_merged[$i]->{end} - $read_merged[$i]->{start};
            print(RFILE
            "$read_merged[$i]->{bytes}\t$read_merged[$i]->{start}\t$read_merged[$i]->{end}\t$duration\t$idle_after\n");
        }
    }

    close(RFILE);

    print(SUMMARY
    "$uid\t$jobid\t\"$start_a\"\t\"$end_a\"\t$start\t$end\t$nprocs\t$rbytes\t$wbytes\n"); 
}

sub main
{
    my @paths;

    if($#ARGV < 0) {
        die("usage: darshan-gather-intervals.pl <one or more log directories>\n");
    }

    @paths = @ARGV;

    if(!(open(SUMMARY, ">>summary.txt")))
    {
        print(STDERR "Failed to open summary.txt\n");
        return;
    }

    print(SUMMARY "#<uid>\t<jobid>\t<start ascii>\t<end ascii>\t<start unix>\t<end unix>\t<nprocs>\t<read bytes>\t<write bytes>\n"); 

    find({wanted=>\&wanted, no_chdir => 1}, @paths);

}

main();

# Local variables:
#  c-indent-level: 4
#  c-basic-offset: 4
# End:
#  
# vim: ts=8 sts=4 sw=4 expandtab
