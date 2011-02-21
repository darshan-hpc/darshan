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

    # only operate on darshan log files
    $file =~ /\.darshan\.gz$/ or return;    

    if(!(open(SUMMARY, "darshan-file-counter-hack.pl $file |")))
    {
        print(STDERR "Failed to parse $File::Find::name\n");
        return;
    }

    while ($line = <SUMMARY>) {
        print($line);
    }

    close(SUMMARY);
}

sub main
{
    my @paths;

    if($#ARGV < 0) {
        die("usage: darshan-gather-stats.pl <one or more log directories>\n");
    }

    @paths = @ARGV;

    print("#<jobid>\t<opened ct>\t<opened avg sz>\t<opened max sz>\t<ro ct>\t<ro avg sz>\t<ro max sz>\t<wo ct>\t<wo avg sz>\t<wo max sz>\t<rw ct>\t<rw avg sz>\t<rw max sz>\t<created ct>\t<created avg sz>\t<created max sz>\n");

    find(\&wanted, @paths);

}

main();

# Local variables:
#  c-indent-level: 4
#  c-basic-offset: 4
# End:
#  
# vim: ts=8 sts=4 sw=4 expandtab
