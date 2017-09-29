#!/usr/bin/perl -w

# This script will go through all of the darshan logs in a given
# subdirectory and summarize a few basic statistics about data usage and
# performance, producing a text file with text in columns

#<jobid> <version> <start ascii> <end ascii> <start unix> <end unix> <nprocs> <bytes read> <bytes written> <perf estimate> 

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
    my $nfiles = 0;

    # only operate on darshan log files
    $file =~ /\.darshan$/ or return;    

    # grab jobid from name, old logs don't store it in the file
    if($file =~ /_id(\d+)_/) {
        $jobid = $1;
    }

    if(!(open(SUMMARY, "darshan-parser --total --perf --file $file |")))
    {
        print(STDERR "Failed to parse $File::Find::name\n");
        return;
    }

    while ($line = <SUMMARY>) {
        if($line =~ /log version: (\S+)/) {
            $version = $1;
        }
        if($line =~ /nprocs: (\S+)/) {
            $nprocs = $1;
        }
        if($line =~ /start_time: (\S+)/) {
            $start = $1;
        }
        if($line =~ /end_time: (\S+)/) {
            $end = $1;
        }
        if($line =~ /total_POSIX_BYTES_READ: (\S+)/) {
            $bytes_r = $1;
        }
        if($line =~ /total_POSIX_BYTES_WRITTEN: (\S+)/) {
            $bytes_w = $1;
        }
        if($line =~ /agg_perf_by_slowest: (\S+)/) {
            $perf = $1;
        }
        if($line =~ /total: (\S+)/) {
            $nfiles = $1;
        }
        if($line =~ /start_time_asci: (.+)/) {
            $start_a = "$1";
        }
        if($line =~ /end_time_asci: (.+)/) {
            $end_a = "$1";
        }
    }

    close(SUMMARY);
    print("$jobid\t$version\t\"$start_a\"\t\"$end_a\"\t$start\t$end\t$nprocs\t$bytes_r\t$bytes_w\t$perf\t$nfiles\n"); 
}

sub main
{
    my @paths;

    if($#ARGV < 0) {
        die("usage: darshan-gather-stats.pl <one or more log directories>\n");
    }

    @paths = @ARGV;

    print("<jobid>\t<version>\t<start ascii>\t<end ascii>\t<start unix>\t<end unix>\t<nprocs>\t<bytes read>\t<bytes written>\t<perf estimate>\t<nfiles>\n"); 

    find(\&wanted, @paths);

}

main();

# Local variables:
#  c-indent-level: 4
#  c-basic-offset: 4
# End:
#  
# vim: ts=8 sts=4 sw=4 expandtab
