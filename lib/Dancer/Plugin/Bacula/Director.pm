#
# This file is part of Dancer-Plugin-Bacula-Director
#
package Dancer::Plugin::Bacula::Director;

# ABSTRACT: easy database connections for Dancer applications

use strict;
use warnings;
our $VERSION = '0.001';    # VERSION
use Carp;
use Dancer::Plugin;
use Try::Tiny;
use Bacula::Director 0.001;

my $_settings;
my $_handles;

sub bacula_director {
    my @args = @_;
    my ( undef, $name ) = plugin_args(@args);
    $name = "_default" if not defined $name;
    return $_handles->{$name} if exists $_handles->{$name};

    $_settings ||= plugin_setting;

    my $conf
        = $name eq '_default'
        ? $_settings
        : $_settings->{connections}->{$name};
    croak "$name is not defined in your bacula_director conf, please check the doc"
        unless defined $conf;

    return $_handles->{$name} = Bacula::Director->new(
        server    => $conf->{server},
        debug     => $conf->{debug},
        encoding  => $conf->{encoding},
        reconnect => $conf->{reconnect} // 60,
        password  => $conf->{password},
    );

}

register bacula_director => \&bacula_director;
register_plugin for_versions => [ 1, 2 ];

1;

__END__
