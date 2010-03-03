#!/usr/bin/perl -w
#
#  (C) 2010 by Argonne National Laboratory.
#      See COPYRIGHT in top-level directory.
#

use Cwd;
use Getopt::Long;
use English;
use File::Temp qw(tempfile);

my $hostfile;
my $dirfile;
my $restartfile;
my $shareddir;
my @hosts;
my @dirs;
my %pidmapping = ();
my %pidmapping_dir = ();
my %pidmapping_time = ();
my %dirtiming = ();
my %chkpntlog = ();

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

open(FILE, $restartfile) or goto SKIP;
while (<FILE>)
{
    my $dir;
    my $checkpoint;
    my $status;
    my $elapsedtime;
    chomp;
    ($status, $elapsedtime, $checkpoint, $dir) = split(/,/, $_, 4);
    $chkpntlog{$dir} = [$checkpoint, $status, $elapsedtime];
}
close(FILE);
SKIP:

my $process_forked = 0;

# loop as long as we still have directories to process or outstanding jobs
ITERATION: while ($#dirs > -1 || keys(%pidmapping))
{
    if($#hosts > -1 && $#dirs > -1)
    {
        # we have work to do and a free host to do it on

        # grab a directory and host out of the lists
        my $dir  = pop(@dirs);
        my $need_restart = 0;

        if (not defined $chkpntlog{$dir})
        {
            my ($fd, $chkname) = tempfile("checkpoints/chkXXXXXXXX", DIR=>$shareddir,
                                          SUFFIX=>".fsstats");
            close($fd);
            $chkpntlog{$dir} = [$chkname, 0, 0];
            $need_restart = 0;
        }
        else
        {
            $need_restart = 1;
        }

        my $info = $chkpntlog{$dir};

        $dirtiming{$dir} = $info->[2];

        #
        # Check to see if this dir is completed
        #
        if ($info->[1] == 1)
        {
            print("dir completed: $dir\n");
            next ITERATION;
        }
        if ($need_restart == 1 && $info->[1] == 0)
        {
            print("restarting dir: $dir\n");
        }

        my $host = pop(@hosts);

        my $pid = fork();
        if (not defined $pid) 
        {
            die("Error: could not fork.");
        }
            
        my $cmd = "./fsstats-runner.bash $host $dir $info->[0] $need_restart";

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
            $pidmapping_time{$pid} = time();
            $process_forked = 1;
        }
    }
    else
    {
        # we have launched as much as we can; wait for something to finish

        if ($process_forked)
        {

        my $child = waitpid(-1, 0);
        if($child < 1)
        {
            die("Error: lost track of a child process.\n");
        }
        my $child_error_code = $?;
        my $elapsedtime = time() - $pidmapping_time{$child};
        print("fsstats of $pidmapping_dir{$child} on $pidmapping{$child} ");
        if($child_error_code == 0)
        {
            print(" SUCCESS.\n");
        }
        else
        {
            print(" FAILURE [$child_error_code] (continuing).\n");
        }

        # update checkpoint
        my $update = $chkpntlog{$pidmapping_dir{$child}};
        $update->[1] = 1;
        $update->[2] = $elapsedtime;
        $chkpntlog{$pidmapping_dir{$child}} = $update;
        dump_checkpoint($restartfile, \%chkpntlog);
        
        # store total time to process directory
        $dirtiming{$pidmapping_dir{$child}} = $elapsedtime;

        # put the host back on the queue
        push(@hosts, $pidmapping{$child});
        delete($pidmapping{$child});
        delete($pidmapping_dir{$child});
        delete($pidmapping_time{$child});

        }
    }
}

foreach my $key (sort { $dirtiming{$b} cmp $dirtiming{$a} } keys %dirtiming)
{
    print "$key $dirtiming{$key}\n";
}

exit 0;

#
# Argument Parsing
#
sub process_args
{
    my $help_switch;

    Getopt::Long::Configure("no_ignore_case", "bundling");
    GetOptions( "help" => \$help_switch,
                "restart=s" => \$restartfile,
                "shareddir=s" => \$shareddir );

    if($help_switch)
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

#
# Dumps checkpoint to restart file.
# Rewrites the whole file.
#
sub dump_checkpoint
{
    my $file = shift;
    my $hash = shift;

    open (CHKPNT, ">", $file) or die "checkpoint failed: $file\n";
    foreach my $key (keys %$hash)
    {
        my $item = $hash->{$key};
        #print CHKPNT "$key,$item->[0],$item->[1]\n";
        print CHKPNT "$item->[1],$item->[2],$item->[0],$key\n";
    }
    close (CHKPNT);
}

#
# Usage info
#
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

