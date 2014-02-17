package Moocula;
use Dancer ':syntax';
use Dancer::Plugin::Bacula::Director;
use Data::Dumper;
use strict;
use warnings;
use Cwd;
use Sys::Hostname;
our $VERSION = '0.1';

set auto_reload => true;
set warnings => false;
set traces => true;

get '/' => sub {
    template 'Moocula/index';
};

get '/browse' => sub {
    my $client = param('client') || "";
    my $jobid = param('id') || "";
    my $pathid = param('pathid') || "";

    my @files;

    my $selector = $pathid ? "pathid=$pathid" : 'path="/"';

    foreach my $line (bacula_director->bvfs_lsdirs("$selector jobid=$jobid"), bacula_director->bvfs_lsfiles("$selector jobid=$jobid")) {
      my ($PathId, $FilenameId, $FileId, $JobId, $LStat, $Path) = split(/\t/, $line);
      push @files, {
        pathid => $PathId,
        filenameid => $FilenameId,
        path => $Path
      }
    }
    template 'Moocula/browse.tt', { client => $client, jobid => $jobid, files => \@files };
};

get '/client' => sub {
    my $client = param('client') || "";
    my @backups;
    foreach my $fileset (bacula_director->filesets) {
      foreach my $line (bacula_director->backups("client=$client fileset=$fileset")) {
        my ($fart, $jobid, $client, $level, $timestamp, $numfiles, $numbytes, $volumeid, $storage) = split(/\ *\|\ */, $line);

        unshift @backups, { 
          jobid => $jobid,
          client => $client,
          timestamp => $timestamp,
          numfiles => $numfiles,
          volumeid => $volumeid,
          storage => $storage
       }
      }
    }
      
    template 'Moocula/client.tt', { client => $client, backups => \@backups };
};

get '/clients' => sub {
    template 'Moocula/clients.tt', { clients => [bacula_director->clients] };
};

true;
