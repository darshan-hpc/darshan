#!/usr/bin/perl -w
#
#  (C) 2010 by Argonne National Laboratory.
#      See COPYRIGHT in top-level directory.
#

use Cwd;
use Getopt::Long;
use English;

my $hostfile;
my $dirfile;
my @hosts;
my @dirs;
my %pidmapping = ();
my %pidmapping_dir = ();

process_args();

open(FILE, $hostfile) or die("Error opening hostfile.");
# read file into an array
@hosts = <FILE>;
chomp(@hosts);
close(FILE);

open(FILE, $dirfile) or die("Error opening dirfile.");
# read file into an array
@dirs = <FILE>;
chomp(@dirs);
close(FILE);

# loop as long as we still have directories to process or outstanding jobs
while ($#dirs > -1 || keys(%pidmapping))
{
    if($#hosts > -1 && $#dirs > -1)
    {
        # we have work to do and a free host to do it on

        # grab a directory and host out of the lists
        my $dir = pop(@dirs);
        my $host = pop(@hosts);

        my $pid = fork();
        if (not defined $pid) 
        {
            die("Error: could not fork.");
        }
            
        my $cmd = "./fsstats-runner.bash $host $dir";

        if($pid == 0)
        {
            # child runs remote command
            my $error_code = 0;
            system($cmd);

            # look for exec problem, signal, or command error code
            if (($? == -1) || ($? & 127) || (($? >> 8) != 0))
            {
                $error_code = 1;
            }
            else
            {
                $error_code = 0;
            }

            # propagate an error code to parent
            exit($error_code);
        }
        else
        {
            print("fsstats of $dir on $host started...\n");
            # parent keeps up with what child is running where
            $pidmapping{$pid} = $host;
            $pidmapping_dir{$pid} = $dir;
        }
    }
    else
    {
        # we have launched as much as we can; wait for something to finish

        my $child = waitpid(-1, 0);
        if($child < 1)
        {
            die("Error: lost track of a child process.\n");
        }
        my $child_error_code = $?;
        print("fsstats of $pidmapping_dir{$child} on $pidmapping{$child} ");
        if($child_error_code == 0)
        {
            print(" SUCCESS.\n");
        }
        else
        {
            print(" FAILURE (continuing).\n");
        }
        
        # put the host back on the queue
        push(@hosts, $pidmapping{$child});
        delete($pidmapping{$child});
        delete($pidmapping_dir{$child});
    }
}

sub process_args
{
    use vars qw( $opt_help );

    Getopt::Long::Configure("no_ignore_case", "bundling");
    GetOptions( "help" );

    if($opt_help)
    {
        print_help();
        exit(0);
    }

    # there should be two remaining arguments (hostfile and dirfile)
    if($#ARGV != 1)
    {
        print "Error: invalid arguments.\n";
        print_help();
        exit(1);
    }

    $hostfile = $ARGV[0];
    $dirfile = $ARGV[1];

    return;
}

sub print_help
{
    print <<EOF;

Usage: $PROGRAM_NAME <hostfile> <dirfile>

    --help          Prints this help message

Purpose:

    This script runs parallel copies of fsstats on each directory listed in
    the <dirfile>.  The <hostfile> specifies a list of hosts to run fsstats
    on via ssh.

EOF
    return;
}

