#!/usr/bin/perl -w
#
#  (C) 2011 by Argonne National Laboratory.
#      See COPYRIGHT in top-level directory.

# takes a single darshan log as input; expectation is that the log is from
# the public data repo and has been annotated with compute node information.
# Prints 3 columns: file name, compute nodes, and run time


sub main()
{
    my $logfile;
    my $header;
    my $cn;
    my $runtime;

    $logfile=$ARGV[0];

    $header = `darshan-parser $logfile |head -n 50`;

    if($header =~ /cn = (\d+)/){
        $cn = $1;
        if($header =~ /run time: (\d+)/){
            $runtime = $1;
            print("$logfile\t$cn\t$runtime\n");
        }
    }

    return 0;
}

#
# Main
#
&main
