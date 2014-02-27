package Bacula;

use warnings;
use strict;
use Data::Dumper;

sub sqlquery {
  my $self    = shift;
  my $command = join(' ', @_);

  $self->__write("sqlquery");
  $self->__write($command);

  $self->__read_lines;
  my @clilines = $self->__read_lines;
  my @row = ();
  my @lines= ();
  my $columnheader = 1;
  foreach my $line (@clilines) {
    if ($line =~ /^You have messages\.$/) { next; }
    if ($line =~ /^Using Catalog/) { next; }
    if ($line =~ /^Enter SQL query:/) { next; }
    if ($line =~ /^ fileindex/) { next; }
    if ($line =~ /^ volumename/) { next; }
    if ($line =~ /^ name/) { next; }
    if ($line eq '+') { next; }
    if ($line eq '-') { next; }
    if ($line eq '|') { next; }
    if ($line eq '' and scalar @row) { # And of row
      push @lines, [@row];
      @row = (); 
    } elsif ($line) {
      $line = substr($line, 1, -2);
      push (@row, $line);
    }
  }
  $self->__write("\n");
  $self->__read_lines;

  return @lines;

}

1

__END__

