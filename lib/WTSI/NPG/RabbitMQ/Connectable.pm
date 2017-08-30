package WTSI::NPG::RabbitMQ::Connectable;

use strict;
use warnings;
use Moose::Role;

use Cwd qw[abs_path];
use JSON;
use File::Slurp qw[read_file];

use Net::AMQP::RabbitMQ;

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable';

has 'hostname' =>
  (is       => 'ro',
   isa      => 'Str',
   lazy     => 1,
   builder  => '_build_hostname',
   documentation => 'Host name or IP address of RabbitMQ server. '.
       'Defaults to the environment variable NPG_RMQ_HOST '.
       ' or "localhost" if the variable is not set.'
);

has 'connection_opts' =>
  (is       => 'ro',
   isa      => 'HashRef',
   lazy     => 1,
   builder  => '_build_connection_opts',
   documentation => 'Options for the second argument to connect() in '.
       'Net::AMQP::RabbitMQ. If the ssl option is true, and ssl_cacert '.
       'is not set, ssl_cacert is set to a default value.',
);

has 'rmq_config_path' =>
    (is       => 'ro',
     isa      => 'Maybe[Str]',
     default  => $ENV{'NPG_RMQ_CONFIG'},
     documentation => 'Path to a JSON config file, which '.
         'contains values for the connection_opts attribute. '.
         'Attribute defaults to the environment variable '.
         'NPG_RMQ_CONFIG if set, Net::AMQP::RabbitMQ internal '.
         'defaults otherwise.',
);

has 'rmq' =>
  (is       => 'ro',
   isa      => 'Net::AMQP::RabbitMQ',
   lazy     => 1,
   builder  => '_build_rmq',
   init_arg => undef,
  );

my $SSL_CACERT_DEFAULT = $ENV{'HOME'}.'/.ssh/ssl-cert-snakeoil.pem';

sub _build_connection_opts {
    my ($self, ) = @_;
    my $opts= {};
    if (defined $self->rmq_config_path) {
        $opts = from_json(read_file($self->rmq_config_path));
    }
    if ($opts->{'ssl'}) {
        $opts->{'ssl_cacert'} ||= $SSL_CACERT_DEFAULT;
    }
    return $opts;
}

sub _build_hostname {
    my ($self, ) = @_;
    my $host = $ENV{'NPG_RMQ_HOST'} || 'localhost';
    return $host;
}

sub _build_rmq {
    my ($self, ) = @_;
    my $rmq = Net::AMQP::RabbitMQ->new();
    return $rmq;
}

# methods have rmq_ prefix to avoid clashes in consuming classes

=head2 rmq_connect

  Args       : None
  Example    : $irods->rmq_connect
  Description: Check connection status; if appropriate, establish a
               connection to the RabbitMQ server with object parameters.

=cut

sub rmq_connect {
    my ($self, ) = @_;

    if ($self->rmq->is_connected()) {
        $self->logwarn('Attempted to connect to RabbitMQ server, but '.
                       'connection already exists; no action taken.');
    } else {
        # return value of connect() not documented
        # try/catch has been found to obscure error messages
        # use is_connected to check status instead
        if (! defined $self->rmq_config_path) {
            $self->logwarn('No RMQ config path given; connecting to ',
                           'server with Net::AMQP::RabbitMQ internal ',
                           'default options');
        }
        $self->rmq->connect($self->hostname, $self->connection_opts);
        if (! $self->rmq->is_connected()) {
            $self->logcroak('Failed to connect to RabbitMQ: ', $!);
        }
        $self->debug('Connected to RabbitMQ server: ',
                     $self->rmq_cluster_name());
    }
    return 1;
}


=head2 rmq_connect

  Args       : None
  Example    : $irods->rmq_connect
  Description: Check connection status; if appropriate, disconnect from
               the RabbitMQ server.

=cut

sub rmq_disconnect {
    my ($self, ) = @_;
    if ($self->rmq->is_connected()) {
        # disconnect() return value not documented
        # try/catch may obscure error message
        # use is_connected to check status instead
        my $name = $self->rmq_cluster_name();
        $self->rmq->disconnect();
        if ($self->rmq->is_connected()) {
            $self->logcroak('Failed to disconnect from RabbitMQ server ',
                            $name);
        }
        $self->debug('Disconnected from RabbitMQ server: ', $name);
    } else {
        $self->logwarn('Attempted to disconnect from RabbitMQ server, but '.
                       'connection does not exist; no action taken.');
    }
    return 1;
}

=head2 rmq_connect

  Args       : None
  Example    : $irods->rmq_connect
  Description: If connected to a RabbitMQ server, return its cluster name;
               otherwise return undef.
  Returntype : Maybe[Str]

=cut

sub rmq_cluster_name {
    my ($self, ) = @_;
    my $name;
    if ($self->rmq->is_connected()) {
        my $props = $self->rmq->get_server_properties();
        $name = $props->{'cluster_name'};
    } else {
        $self->logwarn('RabbitMQ server not connected, cluster name is ',
                       'not available');
    }
    return $name;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::RabbitMQ::Connectable

=head1 DESCRIPTION

Role for connecting to, and disconnecting from, a RabbitMQ server.

=head2 Environment variables

Behaviour of this Role can be configured using the following environment
variables:

=over

=item

NPG_RMQ_HOST: Name or IP address of a RabbitMQ host.

=item

NPG_RMQ_CONFIG: Path to a JSON file with RabbitMQ connection
parameters. Contents of the JSON file are supplied as the second
argument to connect() in Net::AMQP::RabbitMQ.

=back

=head1 AUTHOR

Iain Bancarz <ib5@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2017 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
