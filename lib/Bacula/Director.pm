package Bacula::Director;
use parent 'Bacula';

# Scaffolding copied from Redis.pm

use strict;
use warnings;
use Switch;
our $VERSION = '0.001';
use Carp;
use Try::Tiny;
use IO::Socket::UNIX;
use Digest::HMAC_MD5 qw(hmac_md5 hmac_md5_hex);
use Digest::MD5 qw(md5 md5_hex md5_base64);
use Scalar::Util qw(looks_like_number);

sub new {
  my ($class, %args) = @_;
  my $self  = bless {}, $class;

  $args{debug}
    and $self->{debug} = $args{debug};

  $args{password}
    and $self->{password} = $args{password};

  $args{on_connect}
    and $self->{on_connect} = $args{on_connect};

  defined $args{name}
    and $self->{name} = $args{name};

  $self->{reconnect}     = $args{reconnect} || 0;
  $self->{every}         = $args{every} || 1000;

  $self->{server} = $args{server};
  $self->{builder} = sub {
    my ($self) = @_;

    IO::Socket::INET->new(
      PeerAddr => $self->{server},
      Proto    => 'tcp',
    );
  };

  $self->connect;

  return $self;

};

sub __read_lines {
  my $self = $_[0];
  my $sock = $self->{sock};

  my @lines;

  while (1) {
    my $line = $self->__read_line;
    if (looks_like_number($line) && $line < 0) { warn "DONE!\n"; last; }
    if ($line =~ /^You have messages\.$/) { next; }
    if ($line =~ /^Using Catalog/) { next; }
    push (@lines, $line);
  }
  
  return @lines;
}

sub __read_line {
  my $self = $_[0];
  my $sock = $self->{sock};

  my $header;
  $sock->read($header, 4);
  my $length = unpack 's', pack 'S', unpack "N", $header;

  if ($length < 0) {
    warn "[RECV] length = $length" if $self->{debug};
    return $length;
  }

  my $data;
  $sock->read($data, $length);
  confess("Error while reading from Bacula server: $!")
    unless defined $data;

  chomp $data;
  warn "[RECV] '$data'" if $self->{debug};

  return $data;
}

sub __write {
  my $self = shift;
  my $sock = $self->{sock};
  my $data = shift;
  my $footer = shift;

  warn "[SEND] '$data'" if $self->{debug};

  $footer = "\n" if not defined $footer;
  $data = $data.$footer;
  my $header = pack("N", length $data);

  my $buf = $header.$data;
  while ($buf) {
    my $len = syswrite $sock, $buf, length $buf;
    $self->__throw_reconnect("Could not write to Bacula director: $!")
      unless defined $len;
    substr $buf, 0, $len, "";
  }

  return;
}

### we don't want DESTROY to fallback into AUTOLOAD
sub DESTROY { }

### Deal with common, general case, Bacula-Director commands
our $AUTOLOAD;

sub AUTOLOAD {
  my $command = $AUTOLOAD;
  $command =~ s/.*://;
  $command =~ s/^dot/\./;

  my $method = sub { shift->__std_cmd($command, @_) };

  # Save this method for future calls
  no strict 'refs';
  *$AUTOLOAD = $method;

  goto $method;
}

sub __std_cmd {
  my $self    = shift;
  my $command = shift;
  my @cmd_args = @_;

  ## Fast path, no reconnect;
  #$self->{reconnect} or
  return $self->__run_cmd($command, @cmd_args);

  $self->__with_reconnect(
    sub {
      $self->__run_cmd($command, @cmd_args);
    }
  );
} 

sub __run_cmd {
  my $self    = shift;
  my $command = join(' ', @_); 

  $self->__write($command);

  return $self->__read_lines;
} 

### Socket operations
sub connect {
  my ($self) = @_;
  delete $self->{sock};

  ## Fast path, no reconnect
  return $self->__build_sock() unless $self->{reconnect};

  ## Use precise timers on reconnections
  require Time::HiRes;
  my $t0 = [Time::HiRes::gettimeofday()];

  ## Reconnect...
  while (1) {
    eval { $self->__build_sock };

    last unless $@;    ## Connected!
    die if Time::HiRes::tv_interval($t0) > $self->{reconnect};    ## Timeout
    Time::HiRes::usleep($self->{every});                          ## Retry in...
  }

  return;
}

sub __build_sock {
  my ($self) = @_;

  $self->{sock} = $self->{builder}->($self)
    || confess("Could not connect to Bacula-Director server at $self->{server}: $!");

  if (exists $self->{password}) {
    try { $self->__auth($self->{password}) }
    catch {
      $self->{reconnect} = 0;
      confess("Bacula-Director server refused password");
    };
  }

  return;
}

sub __auth {
  my $self = shift;
  my $pass = shift;
  
  $self->__write("Hello *UserAgent* calling");
  my $response = $self->__read_line;

  my $challenge; my $tls_remote_need;
  my $compat;
  
  ($challenge, $tls_remote_need) = ($response =~ /auth\s+cram\-md5\s+(\S*)\s+ssl\=\s*([-+]?\d+(?:_\d+)*)/);

  $pass = md5_hex($pass);
  my $hmac = Digest::HMAC_MD5->new($pass);
  $hmac->add($challenge);
  my $digest = $hmac->b64digest;
  $self->__write($digest."\0", "");
  $response = $self->__read_line;

  $self->__write("auth cram-md5 foo ssl=0");
  $response = $self->__read_line;
  $self->__write("1000 OK auth");
  $response = $self->__read_line;
}

1;

__END__
