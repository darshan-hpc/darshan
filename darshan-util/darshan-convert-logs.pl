#!/usr/bin/env perl

#
# Copyright (C) 2015 University of Chicago.
# See COPYRIGHT notice in top-level directory.
#

# Convert a list of logs and add metadata from another file.
#
# List of Logs: logfile path, one per line.  This can be generated with a 
#           command like "find <path> -name "*.gz"".  Make sure that the
#           resulting list does not include .partial files.
#	/path/to/log1
#	/path/to/log2
#
# Metadata: List of jobids with additional metadata, one per line, items are
#           tab separated.
#	jobid1	key1=val1	key2=val2
#	jobid2	key1=val1	key2=val2
#

use warnings;

my $darshan_convert = "./darshan-convert";
my $jenkins_hash_gen = "./jenkins-hash-gen";

sub load_annotations($$)
{
    my $fname = shift;
    my $ref = shift;
    my $line;

    open(FH, "<$fname") or die("Failed to open annotations: $fname\n");
    while($line=<FH>)
    {
        my ($jobid, $annotation) = split(/\t/,$line,2);
        chomp($annotation);
        $ref->{$jobid} = $annotation;
    }
    close(FH);
}

sub main()
{
    my $annotation_list;
    my $logfile_list;
    my $hash_key;
    my $output_path;
    my $logfile;
    my $ref = {};

    $hash_key=$ARGV[0];
    $annotation_list=$ARGV[1];
    $logfile_list=$ARGV[2];
    $output_path=$ARGV[3];

    load_annotations($annotation_list, $ref);

    open(LOGFILES, "<$logfile_list") or die("Can't open: $logfile_list");
    while($logfile=<LOGFILES>)
    {
        chomp($logfile);
        if ($logfile =~ /_id(\d+)_/)
        {
            my $jobid;
            my $annotation;
            my $hashed_fname;
            my @args;
            my $year;
            my $month;
            my $day;
            my $logname;
            my $rc;

            $jobid = $1;
            $annotation = $ref->{$jobid};

            if ($logfile =~ /\/(\d+)\/(\d+)\/(\d+)\/([0-9a-zA-Z\-_\.]+)/)
            {
                $year = $1;
                $month = $2;
                $day = $3;
                $logname = $4;
            }
            $hashed_fname = `$jenkins_hash_gen --64 --key $hash_key $logname`;
            chomp($hashed_fname);

            @args = ("$darshan_convert",
                     "--bzip2",
                     "--obfuscate",
                     "--key=$hash_key",
                     "--annotate=$annotation",
                     "--reset-md",
                     "$logfile",
                     "$output_path/$year/$month/$day/$hashed_fname.darshan");
            $rc = system(@args);
            if ($rc) {
                print("$hashed_fname\t$logfile:failed:$rc\n");
            }
            else {
                print("$hashed_fname\t$logfile\n");
            }
        }
        else
        {
            print("Invalid logfile name: $logfile\n")
        }
    }

    return 0;
}

#
# Main
#
&main
