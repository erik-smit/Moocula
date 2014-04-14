package Bacula::Storage;

# Scaffolding copied from Redis.pm

use strict;
use warnings;
use Switch;
our $VERSION = '0.001';
use Carp;
use Try::Tiny;
use IO::Socket::UNIX;
use IO::Uncompress::AnyInflate qw(anyinflate $AnyInflateError) ;
use Digest::HMAC_MD5 qw(hmac_md5 hmac_md5_hex);
use Digest::MD5 qw(md5 md5_hex md5_base64);
use Data::Dumper;

sub new {
  my ($class, %args) = @_;
  my $self  = bless {}, $class;

  $args{debug}
    and $self->{debug} = $args{debug};

  $args{director}
    and $self->{director} = $args{director};

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

sub __read_msgs {
  my $self = $_[0];
  my $sock = $self->{sock};

  my @lines;

  while (my $line = $self->__read_msg) {
    if ($line =~ /^You have messages\.$/) { next; }
    if ($line =~ /^Using Catalog/) { next; }
    push (@lines, $line);
  }
  
  return @lines;
}

sub __read_msg {
  my ($self) = @_;
  my $sock = $self->{sock};

  warn "[RECV] waiting for data.\n" if $self->{debug};
  my $header;
  $sock->sysread($header, 4);
  my $length = unpack "N", $header;
  warn "[RECV] length = ".Dumper($length)."length" if $self->{debug};
  if ($length >= 4294967295) {
    return;
  }

  my $data;
  $sock->read($data, $length);
  confess("Error while reading from Bacula server: $!")
    unless defined $data;

  chomp $data;
  warn "[RECV] '$data'" if $self->{debug};

  return $data;
}

sub __read_rec {
  my ($self) = @_;
  my $sock = $self->{sock};
  
  my $filedata;
  my $olddebug = $self->{debug};
  delete $self->{debug};
  while (!$filedata) {
    my $rechdr = $self->__read_msg;

    if (!$rechdr) { last; }

    my  ($VolSessionId, $VolSessionTime, $file_index, $stream, $length) =
       ($rechdr =~ /^rechdr\s(\d+)\s(\d+)\s(\d+)\s(\d+)\s(\d+)$/);

    my $data = $self->__read_msg;

    if ($stream == 4) {
      anyinflate \$data => \$filedata
        or die "trying\n";
    }
  }
  $self->{debug} = $olddebug;

  return $filedata;
}

sub __write {
  my ($self, $data, $footer) = @_;
  my $sock = $self->{sock};

  warn "[SEND] '$data'" if $self->{debug};

  $footer = "\n" if not defined $footer;
  $data = $data.$footer;
  my $header = pack("N", length $data);

  my $buf = $header.$data;
  while ($buf) {
    my $len = syswrite $sock, $buf, length $buf;
    #$self->__throw_reconnect("Could not write to Bacula storage: $!")
    return 
      unless defined $len;
    substr $buf, 0, $len, "";
  }

  return;
}

### we don't want DESTROY to fallback into AUTOLOAD
sub DESTROY { }

### Deal with common, general case, Bacula-Storage commands
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

  return $self->__read_msgs;
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
    || confess("Could not connect to Bacula-Storage server at $self->{server}: $!");

#  if (exists $self->{password} && exists $self->{director}) {
  try { $self->__auth("Hello Director $self->{director} calling", md5_hex($self->{password})) }
  catch {
    $self->{reconnect} = 0;
    confess("Bacula-Storage server refused password");
  };
  
  return;
}

sub lezen_pre {
  my ($self, $volume, $fileindex) = @_;

  my $job_cmd = "JobId=%d job=%s job_name=%s client_name=%s ".
    "type=%d level=%d FileSet=%s NoAttr=%d SpoolAttr=%d FileSetMD5=%s ".
    "SpoolData=%d WritePartAfterJob=%d PreferMountedVols=%d SpoolSize=%s ".
    "rerunning=%d VolSessionId=%d VolSessionTime=%d";

  my $cmd = sprintf($job_cmd, 50, "012345678901234567890", "foo", "foo", 82, 70, " ", 0, 0, 0, "", 0, 0, 0, 0, 0, 0, 0);
  $self->__write($cmd);
  my $resp = $self->__read_msg;
  my ($foo, $bar, $baz, $unf) = split(/=/, $resp);

  my $buf = pack 'N', 0xFFFFFFFF;
  $self->__write("use storage=File media_type=File pool_name=Default pool_type=Backup append=0 copy=0 stripe=0");
  $self->__write("use device=FileStorage");
  syswrite $self->{sock}, $buf; #, len $buf;
  syswrite $self->{sock}, $buf; #, len $buf;
  $self->__read_msg;
  $self->__write("bootstrap");
  $self->__write("Volume=\"$volume\"");
#  $self->__write("VolFile=0");
  $self->__write("FileIndex=$fileindex");
#  $self->__write("VolBlock=0-0");
#  $self->__write("Device=\"FileStorage\"");
#  $self->__write("MediaType=\"File\"");
  syswrite $self->{sock}, $buf;
  $self->__write("run");
  $self->__read_msg;

  $self->{dirsock} = $self->{sock};

  $self->{fdsock} = $self->{builder}->($self)
    || confess("Could not connect to Bacula-Storage server at $self->{server}: $!");

  warn("Setting up FD-SD connection.\n") if $self->{debug};

  $self->{sock} = $self->{fdsock};
  $self->__auth("Hello Start Job 012345678901234567890", $unf, 1);
  warn("Done setting up FD-SD connection.\n") if $self->{debug};

  $self->__write("read open session = $volume 50 0 0 0 1000 1000");
  # 3000 OK open ticket = 1
  my $resp2 = $self->__read_msg;
  my ($ticket) = ($resp2 =~ /3000 OK open ticket = (\d+)/);
  $self->__write("read data $ticket");
  $self->{sock} = $self->{dirsock};

  # Status Job=012345678901234567890 JobStatus=70
  my $kees = $self->__read_msg;
  # 3010 Job 012345678901234567890 start
  $kees .= $self->__read_msg; 
  # Status Job=012345678901234567890 JobStatus=82
  $kees .= $self->__read_msg;
  # CatReq Job=012345678901234567890 GetVolInfo VolName=Month-2014-02-19-2-vol write=0
  $kees .= $self->__read_msg;
  my ($job, $volname, $write) = ($kees =~ /CatReq Job=(\s+) GetVolInfo VolName=(\s+) write=(\d+)/); 
  my $mkaaay = "1000 OK VolName=%s VolJobs=%u VolFiles=%u".
   " VolBlocks=%u VolBytes=%s VolMounts=%u VolErrors=%u VolWrites=%u".
   " MaxVolBytes=%s VolCapacityBytes=%s VolStatus=%s Slot=%d".
   " MaxVolJobs=%u MaxVolFiles=%u InChanger=%d VolReadTime=%s".
   " VolWriteTime=%s EndFile=%u EndBlock=%u VolParts=%u LabelType=%d".
   " MediaId=%s";
  my $response = sprintf($mkaaay, $volname, 0, 0,
    0, 0, 0, 0, 0,
    0, 0, 0, 0,
    0, 0, 0, 0, 
    0, 0, 0, 0, 0,
    "File");
  $self->__write($response);

  $self->{sock} = $self->{fdsock};

  # Expect: 3000 OK data
  $self->__read_msg;
  
  $self->{_debug} = $self->{debug};
  delete $self->{debug};

  return;
}

sub lezen_stream {
  my ($self) = @_;

  return $self->__read_rec;
}
  
sub lezen_post {
  my ($self) = @_;

  $self->{debug} = $self->{_debug};
  delete $self->{_debug};
  delete $self->{dirsock};
  delete $self->{fdsock};
  $self->connect;
}

   
sub __auth {
  my ($self, $hello, $pass, $skipread) = @_;
  
  $self->__write($hello);
  my $response = $self->__read_msg;
  
  my ($challenge, $tls_remote_need) = ($response =~ /auth\s+cram\-md5\s+(\S*)\s+ssl\=\s*([-+]?\d+(?:_\d+)*)/);
  warn("challenge received: $challenge\n") if $self->{debug};

  my $hmac = Digest::HMAC_MD5->new($pass);
  $hmac->add($challenge);
  my $digest = $hmac->b64digest;
  $self->__write($digest."\0", "");
  $response = $self->__read_msg;

  $self->__write("auth cram-md5 foo ssl=0");
  $response = $self->__read_msg;
  $self->__write("1000 OK auth");
  $response = $self->__read_msg unless $skipread;
}

1;

__END__
