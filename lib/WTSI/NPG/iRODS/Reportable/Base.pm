package WTSI::NPG::iRODS::Reportable::Base;

use strict;
use warnings;
use Moose::Role;

use DateTime;
use File::Basename qw[fileparse];
use JSON;
use Time::HiRes qw[gettimeofday];
use Try::Tiny;

our $VERSION = '';

with 'WTSI::NPG::RabbitMQ::Connectable';

has 'channel' =>
    (is       => 'ro',
     isa      => 'Int',
     default  => 1,
     documentation => 'A RabbitMQ channel',
);

has 'exchange' =>
    (is       => 'ro',
     isa      => 'Str',
     default  => 'npg.gateway',
     documentation => 'A RabbitMQ exchange name',
);

has 'routing_key_prefix' =>
    (is       => 'ro',
     isa      => 'Str',
     default  => 'prod',
     documentation => 'Prefix for the RabbitMQ routing key. May be '.
         'used to distinguish test from production messages.',
);

has 'enable_rmq' =>
    (is       => 'ro',
     isa      => 'Bool',
     lazy     => 1,
     default  => 1,
     documentation => 'If true, publish messages to the RabbitMQ '.
         'server. True by default.',
 );


=head2 collection_message_body

  Arg [1]    : [Str] iRODS collection path
  Arg [2]    : [WTSI::NPG::iRODS] iRODS instance. Optional, defaults to
               $self (useful if the class consuming this Role is a subclass
               of WTSI::NPG::iRODS).

  Example    : $irods->collection_message_body($path, $irods);
  Description: Generates a data structure describing an iRODS collection,
               and encodes as JSON to form the body of a RabbitMQ message.
  Returntype:  HashRef

=cut

sub collection_message_body {
    my ($self, $path, $irods) = @_;
    $irods ||= $self;
    $path = $irods->ensure_collection_path($path);
    my @avus = $irods->get_collection_meta($path);
    # $spec based on json() method of DataObject; also records permissions

    my @permissions = $irods->get_collection_permissions($path);
    my $spec = { collection  => $path,
                 avus        => \@avus,
         acl         => \@permissions,
             };
    my $body = encode_json($spec);
    return $body;
}


=head2 message_headers

  Arg [1]    : [Str] RabbitMQ message body as a JSON string; used to obtain
               the file type AVU (if any)
  Arg [2]    : [Str] name of method called
  Arg [3]    : [Str] timestamp, in format output by rmq_timestamp()
  Arg [4]    : [Str] iRODS username

  Example    : $irods->message_headers($avus, $my_method, $timestamp, $user);
  Description: Generate a HashRef for use as the RabbitMQ message headers.
  Returntype:  HashRef

=cut

sub message_headers {
    my ($self, $body, $method_name, $time, $irods_user) = @_;
    my $type = $self->_type_from_message_body($body);
    my $headers = {
        method     => $method_name, # name of Moose method called
        timestamp  => $time,        # time immediately before method call
        user       => $ENV{USER},   # OS username (may differ from irods_user)
        irods_user => $irods_user,  # iRODS username
        type       => $type,        # file type from metadata, if any
    };
    if (defined $type) {
        $headers->{type} = $type;
    }
    return $headers;
}

=head2 object_message_body

  Arg [1]    : [Str] iRODS data object path
  Arg [2]    : [WTSI::NPG::iRODS] iRODS instance. Optional, defaults to
               $self (useful if the class consuming this Role is a subclass
               of WTSI::NPG::iRODS).

  Example    : $irods->object_message_body($path, $irods);
  Description: Generates a data structure describing an iRODS data object.
               Can be encoded as JSON to form the body of a RabbitMQ message.
  Returntype:  Str

=cut

sub object_message_body {
    my ($self, $path, $irods) = @_;
    $irods ||= $self;
    $path = $irods->ensure_object_path($path); # uses path cache
    my ($obj, $collection, $suffix) = fileparse($path);
    $collection =~ s/\/$//msx; # remove trailing /
    my @avus = $irods->get_object_meta($path); # uses metadata cache
    # $spec based on json() method of DataObject; also records permissions

    my @permissions = $irods->get_object_permissions($path);
    my $spec = { collection  => $collection,
                 data_object => $obj,
                 avus        => \@avus,
         acl         => \@permissions,
             };
    my $body = encode_json($spec);
    return $body;
}


=head2 publish_rmq_message

  Arg [1]    : Message body in JSON string format [Str]
  Arg [2]    : Message headers [HashRef]

  Example    : $irods->publish_rmq_message($body, $headers)
  Description: Publishes a RabbitMQ message to the channel and exchange
               determined by object attributes.

               In order to construct message headers, the method attempts
               to decode the message body string as JSON. If unable to
               do so, it logs a warning.

=cut

sub publish_rmq_message {
    my ($self, $body, $headers) = @_;
    my $key = $self->routing_key_prefix.'.irods.report';
    $self->rmq->publish($self->channel,
                        $key,
                        $body,
                        { exchange => $self->exchange },
                        { headers => $headers },
                    );
    # Net::AMQP::RabbitMQ documentation specifies 'header' as key in
    # props argument to 'publish', but this is incorrect (2017-07-31)
    return 1;
}

=head2 rmq_init

  Args       : None
  Example    : $irods->rmq_init()
  Description: Initialize an RMQ connection by calling the connect
               method and opening a channel.

=cut

sub rmq_init {
    my ($self,) = @_;
    $self->rmq_connect();
    $self->rmq->channel_open($self->channel);
    $self->debug('Server properties: ',
                 encode_json($self->rmq->get_server_properties));
    return 1;
}

=head2 rmq_timestamp

  Args       : None
  Example    : $irods->rmq_timestamp()
  Description: Return a timestamp in seconds since the epoch, precise to
               the microsecond (accuracy depends on the DateTime module).
  Returntype : Str

=cut

sub rmq_timestamp {
    my ($self, ) = @_;
    my ($seconds, $microseconds) = gettimeofday();
    my $time = DateTime->from_epoch(epoch => $seconds);
    my $decimal_string = sprintf "%06d", $microseconds;
    return $time->iso8601().q{.}.$decimal_string;
}

### private methods

sub _type_from_message_body {
    # convenience method to get file type AVU (if any) from message body JSON
    # returns an empty string if AVU is not found
    my ($self, $body) = @_;
    my $response;
    try {
        $response = decode_json($body);
    } catch {
        $self->logwarn(q{Unable to decode JSON from message body: '},
                       $body, q{'});
    };
    my $type = q{};
    foreach my $avu (@{$response->{'avus'}}) {
        if ($avu->{attribute} eq 'type') {
            if ($type) {
                $self->logwarn('More than one file type AVU in message body,',
                               " using '", $type, "': '", $body, q{'});
                last;
            }
            $type = $avu->{value};
        }
    }
    return $type;
}

no Moose::Role;

1;


__END__

=head1 NAME

WTSI::NPG::iRODS::Reportable::Base

=head1 DESCRIPTION

A Role to enable reporting of method calls to a RabbitMQ message server.

=head2 Required methods

The consuming class must have the following methods:

=over

=item

get_irods_user

=back

See the WTSI::NPG::iRODS class for an example.


=head2 Test requirements

Tests for this Role require a working RabbitMQ server. The hostname and
configuration may be set using the environment variables NPG_RMQ_HOST
and NPG_RMQ_CONFIG, otherwise will be given default values.

The RabbitMQ server must have a user with password matching the given
configuration. The user must have read, write, and configure permissions
on the given RabbitMQ virtual host. If SSL is enabled for tests, the user
must also have a valid SSL certificate for connection to the host.

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
