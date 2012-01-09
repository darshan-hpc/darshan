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
    }

    if(!(close(PARSE)))
    {
        print(STDERR "Failed to parse $File::Find::name\n");
        return;
    }
    print(SUMMARY "$jobid\t\"$start_a\"\t\"$end_a\"\t$start\t$end\t$nprocs\n"); 
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

    print(SUMMARY "<jobid>\t<start ascii>\t<end ascii>\t<start unix>\t<end unix>\t<nprocs>\n"); 

    find(\&wanted, @paths);

}

main();

# Local variables:
#  c-indent-level: 4
#  c-basic-offset: 4
# End:
#  
# vim: ts=8 sts=4 sw=4 expandtab
