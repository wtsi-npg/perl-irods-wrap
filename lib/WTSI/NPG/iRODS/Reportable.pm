package WTSI::NPG::iRODS::Reportable;

use strict;
use warnings;
use Moose::Role;

use DateTime;
use JSON;
use WTSI::DNAP::Utilities::Runnable;

our $VERSION = '';

with 'WTSI::NPG::RabbitMQ::Connectable';

# consuming class must have these methods
requires qw[ensure_collection_path
            ensure_object_path
            get_irods_user];

our @REPORTABLE_COLLECTION_METHODS =
    qw[
          add_collection
          put_collection
          move_collection
          set_collection_permissions
          add_collection_avu
          remove_collection_avu
  ];

our @REPORTABLE_OBJECT_METHODS =
    qw[
          add_object
          replace_object
          copy_object
          move_object
          set_object_permissions
          add_object_avu
          remove_object_avu
  ];

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

has 'no_rmq' =>
    (is       => 'ro',
     isa      => 'Bool',
     lazy     => 1,
     builder  => '_build_no_rmq',
     documentation => 'If true, do not connect to the RabbitMQ server. '.
         'True by default unless the rmq_config_path attribute in the '.
         'WTSI::NPG::RabbitMQ::Connectable role is defined.',
 );

after 'BUILD' => sub {
    my ($self, @args) = @_;
    if (! $self->no_rmq) {
        $self->rmq_connect();
        $self->rmq->channel_open($self->channel);
        $self->debug('Server properties: ',
                     encode_json($self->rmq->get_server_properties));
    }
};

after 'DEMOLISH' => sub {
    my ($self, @args) = @_;
    if (! $self->no_rmq) {
        $self->rmq_disconnect();
    }
};

foreach my $name (@REPORTABLE_COLLECTION_METHODS) {

    around $name => sub {
        my ($orig, $self, @args) = @_;
	my $now = DateTime->now()->iso8601();
        my $collection = $self->$orig(@args);
        if (! $self->no_rmq) {
            $self->debug('RabbitMQ reporting for method ', $name,
                         ' on collection ', $collection);
            my $spec = { collection  => $collection };
            $self->_publish_message($spec, $name, $now);
        }
        return $collection;
    };

}

foreach my $name (@REPORTABLE_OBJECT_METHODS) {

    around $name => sub {
        my ($orig, $self, @args) = @_;
	my $now = DateTime->now()->iso8601();
        my $object = $self->$orig(@args);
        if (! $self->no_rmq) {
            $self->debug('RabbitMQ reporting for method ', $name,
                         ' on data object ', $object);
            my ($volume, $collection, $data_name) =
                File::Spec->splitpath($object);
            my $spec = { collection  => $collection,
                         data_object => $data_name  };
            $self->_publish_message($spec, $name, $now);
        }
        return $object;
    };

}

before 'remove_collection' => sub {
    my ($self, @args) = @_;
    if (! $self->no_rmq) {
        my $collection = $self->ensure_collection_path($args[0]);
        $self->debug('RabbitMQ reporting for method remove_collection',
                     ' on collection ', $collection);
        my $spec = { collection  => $collection };
        my $now = DateTime->now()->iso8601();
        $self->_publish_message($spec, 'remove_collection', $now);
    }
};

before 'remove_object' => sub {
    my ($self, @args) = @_;
    if (! $self->no_rmq) {
        my $object = $self->ensure_object_path($args[0]);
        $self->debug('RabbitMQ reporting for method remove_object',
                     ' on data object ', $object);
        my ($volume, $collection, $data_name) =
            File::Spec->splitpath($object);
        my $spec = { collection  => $collection,
                     data_object => $data_name  };
        my $now = DateTime->now()->iso8601();
        $self->_publish_message($spec, 'remove_object', $now);
    }
};

sub _build_no_rmq {
    my ($self, ) = @_;
    my $no_rmq = 1;
    if (defined $self->rmq_config_path) {
        $no_rmq = 0;
    }
    return $no_rmq;
}

sub _get_headers {
    my ($self, $response, $name, $time) = @_;
    my $irods_user = $self->get_irods_user();
    my $headers = {
        method     => $name,       # name of Moose method called
        timestamp  => $time,       # time immediately before method call
        user       => $ENV{USER},  # OS username (may differ from irods_user)
	irods_user => $irods_user, # iRODS username
        type       => q{},         # file type from metadata, if any
    };
    my $response_data = decode_json($response);
    foreach my $avu (@{$response_data->{'avus'}}) {
        if ($avu->{attribute} eq 'type') {
            $headers->{type} = $avu->{value};
        }
    }
    return $headers;
}

sub _list_irods_details {
    my ($self, $spec) = @_;
    my $args = ['--acl', '--avu', '--timestamp', '--replicate'];
    # call to baton and get JSON response
    # must have 'environment' attribute (hashref of env variables)
    # TODO refactor this to avoid starting a new process
    my $runnable = WTSI::DNAP::Utilities::Runnable->new
        (executable  => 'baton-list',
         arguments   => $args,
         environment => $self->environment);
    ${$runnable->stdin} .= encode_json($spec);
    $runnable->run();
    my $response = ${$runnable->stdout};
    return $response;
}

sub _publish_message {
    my ($self, $spec, $name, $now) = @_;
    my $response = $self->_list_irods_details($spec);
    $self->debug('Got response from baton: ', $response);
    my $key = $self->routing_key_prefix.'.irods.data.create';
    my $headers = $self->_get_headers($response, $name, $now);
    $self->rmq->publish($self->channel,
                        $key,
                        $response,
                        { exchange => $self->exchange },
                        { headers => $headers },
                    );
    return 1;
}


no Moose::Role;

1;


__END__

=head1 NAME

WTSI::NPG::iRODS::Reportable

=head1 DESCRIPTION

A Role to enable reporting of method calls to a RabbitMQ message server.

=head2 Required methods

The consuming class must have the following methods:

=over

=item

ensure_collection_path

=item

ensure_object_path

=item

get_irods_user

=back

See the WTSI::NPG::iRODS class for examples.


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
