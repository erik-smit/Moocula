package Moocula;
use Dancer ':syntax';
use Dancer::Plugin::Bacula::Director;
use Dancer::Plugin::Bacula::Storage;
use Data::Dumper;
use strict;
use warnings;
use Cwd;
use Sys::Hostname;
use Scalar::Util qw/looks_like_number/;
use POSIX qw(strftime);

our $VERSION = '0.1';

set auto_reload => true;
set warnings    => false;
set traces      => true;

get '/' => sub {
    template 'Moocula/index';
};

get '/browse' => sub {
    my $client = param('client') || "";
    my $jobid  = param('id')     || "";
    my $pathid = param('pathid') || "";

    my @files;

    my $selector = $pathid ? "pathid=$pathid" : 'path="/"';
    my ($alljobid) = bacula_director->dotbvfs_get_jobids("jobid=$jobid");

    foreach my $line (
        bacula_director->dotbvfs_lsdirs("$selector jobid=$alljobid"),
        bacula_director->dotbvfs_lsfiles("$selector jobid=$alljobid")
      )
    {
        my ( $PathId, $FilenameId, $FileId, $JobId, $LStat, $Path ) =
          split( /\t/, $line );
        push @files,
          {
            pathid => $PathId,
            fileid => $FileId,
            filenameid => $FilenameId,
            path   => $Path
          };
    }
    template 'Moocula/browse.tt',
      { client => $client, jobid => $jobid, files => \@files };
};

get '/client' => sub {
    my $client = param('client') || "";
    my @backups;
    foreach my $fileset ( bacula_director->dotfilesets ) {
        foreach my $line (
            bacula_director->dotbackups("client=$client fileset=$fileset") )
        {
            my (
                $fart,     $jobid,    $client,   $level, $timestamp,
                $numfiles, $numbytes, $volumeid, $storage
            ) = split( /\ *\|\ */, $line );

            unshift @backups,
              {
                jobid     => $jobid,
                client    => $client,
                timestamp => $timestamp,
                numfiles  => $numfiles,
                volumeid  => $volumeid,
                storage   => $storage
              };
        }
    }

    template 'Moocula/client.tt', { client => $client, backups => \@backups };
};

get '/clients' => sub {
    my @clients;

# Only list clients that have backups
#    foreach my $fileset (bacula_director->dotfilesets) {
#      foreach my $client (bacula_director->dotclients) {
#        my @backups = bacula_director->dotbackups("client=$client fileset=$fileset");
#        if (scalar @backups > 0) {
#	  push @clients, $client;
#	}
#      }
#    }
    my @clients = bacula_director->dotclients;

#  @clients = bacula_director->sqlquery("SELECT client.clientid, client.name FROM client INNER JOIN job ON (client.clientid=job.clientid) WHERE job.jobstatus='T' GROUP BY client.clientid ORDER BY client.name");

    template 'Moocula/clients.tt', { clients => [@clients] };
};

get '/download' => sub {
    send_file(
        \"foo",
        streaming => 1,
        callbacks => {
            override => sub {
                my ( $respond, $response ) = @_;

                my $client = param('client') || "";
                my $jobid  = param('id')     || "";
                my $fileid = param('fileid') || "";

                my @files;

                my @lines = bacula_director->sqlquery(
                    "SELECT DISTINCT VolumeName, File.FileIndex, Filename.name
 FROM Job,JobMedia,Media,File,Filename
 WHERE File.FileID=$fileid
 AND File.FilenameID=Filename.FilenameID
 AND File.JobID=Job.JobID 
 AND Job.JobId=JobMedia.JobId
 AND JobMedia.MediaId=Media.MediaId;\n"
                );

                my $volume    = $lines[0][0];
                my $fileindex = $lines[0][1];
                my $filename  = $lines[0][2];

 		$fileindex =~ s/,//g;

                my $http_status_code = 200;
                my @http_headers     = (
                    'Content-Type'        => 'text/plain',
                    'Content-Disposition' => 'attachment; filename="'
                      . $filename . '"',

                    'Last-Modified' =>
                      strftime( "%a, %d %b %Y %H:%M:%S GMT", gmtime ),
                    'Expires' => 'Tue, 03 Jul 2001 06:00:00 GMT',
                    'Cache-Control' =>
                      'no-store, no-cache, must-revalidate, max-age=0',
                    'Pragma' => 'no-cache'
                );

                my $writer =
                  $respond->( [ $http_status_code, \@http_headers ] );

                bacula_storage->lezen_pre($volume, $fileindex);
    
                while(my $data = bacula_storage->lezen_stream()) {
                  $writer->write( $data ); 
                }

                bacula_storage->lezen_post();

            },
        },
    );
};

true;
