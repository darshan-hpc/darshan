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
    my @fields;
    my $mpi_coll_count = 0;
    my $mpi_indep_count = 0;
    my $posix_count = 0;

    # only operate on darshan log files
    $file =~ /\.darshan\.gz$/ or return;    

    # grab jobid from name, old logs don't store it in the file
    if($file =~ /_id(\d+)_/) {
        $jobid = $1;
    }

    if(!(open(SUMMARY, "darshan-parser --file-list-detailed $file |")))
    {
        print(STDERR "Failed to parse $File::Find::name\n");
        return;
    }

    while ($line = <SUMMARY>) {
        if($line =~ /^#/) {
            next;
        }
        if($line =~ /^\s/) {
            next;
        }

        @fields = split(/\s/, $line);

        if($#fields == 34)
        {
            if($fields[13] > 0){
                $mpi_coll_count ++;
            }
            elsif($fields[12] > 0){
                $mpi_indep_count ++;
            }
            elsif($fields[14] > 0){
                $posix_count ++;
            }

        }
    }

    print(STDOUT "$jobid\t$mpi_coll_count\t$mpi_indep_count\t$posix_count\n");
    close(SUMMARY);
}

sub main
{
    my @paths;

    if($#ARGV < 0) {
        die("usage: darshan-gather-stats.pl <one or more log directories>\n");
    }

    @paths = @ARGV;

    print("# <jobid>\t<#files_using_collectives>\t<#files_using_indep>\t<#files_using_posix>\n"); 
    print("# NOTE: a given file will only show up in one category, with preference in the order shown above (i.e. a file that used collective I/O will not show up in the indep or posix category).\n");

    find(\&wanted, @paths);

}

main();

# Local variables:
#  c-indent-level: 4
#  c-basic-offset: 4
# End:
#  
# vim: ts=8 sts=4 sw=4 expandtab
