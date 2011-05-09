#!/usr/bin/perl -w
#
#  (C) 2011 by Argonne National Laboratory.
#      See COPYRIGHT in top-level directory.
#
# Convert a list of logs and add metadata from another file.
#
# List of Logs: logfile path, one per line.
#	/path/to/log1
#	/path/to/log2
#
# Metadata: List of jobids with additional metadata, one per line, items are
#           tab separated.
#	jobid1	key1=val1	key2=val2
#	jobid2	key1=val1	key2=val2
#

my $darshan_convert = "./darshan-convert";

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

            $jobid = $1;
            $annotation = $ref->{$jobid};

            $hashed_fname = `./jenkins --64 --key $hash_key $logfile`;
            chomp($hashed_fname);

            @args = ("$darshan_convert",
                     "--obfuscate",
                     "--key=$hash_key",
                     "--annotate=$annotation",
                     "$logfile",
                     "$output_path/$hashed_fname.gz");
            system(@args);
            print("$hashed_fname\t$logfile\n");
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
