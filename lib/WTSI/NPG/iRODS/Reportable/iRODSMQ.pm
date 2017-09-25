package WTSI::NPG::iRODS::Reportable::iRODSMQ;

use strict;
use warnings;
use Moose::Role;

our $VERSION = '';

with 'WTSI::NPG::iRODS::Reportable::Base';

requires qw[get_collection_meta
            get_object_meta
            ensure_collection_path
            ensure_object_path
];

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


foreach my $name (@REPORTABLE_COLLECTION_METHODS) {

    around $name => sub {
        my ($orig, $self, @args) = @_;
        my $now = $self->rmq_timestamp();
        my $collection = $self->$orig(@args);
        if ($self->enable_rmq) {
            $self->debug('RabbitMQ reporting for method ', $name,
                         ' on collection ', $collection);
            my $body = $self->collection_message_body($collection);
            my $user = $self->get_irods_user;
            my $headers = $self->message_headers($body, $name, $now, $user);
            $self->publish_rmq_message($body, $headers);
        }
        return $collection;
    };

}

foreach my $name (@REPORTABLE_OBJECT_METHODS) {

    around $name => sub {
        my ($orig, $self, @args) = @_;
        my $now = $self->rmq_timestamp();
        my $object = $self->$orig(@args);
        if ($self->enable_rmq) {
            $self->debug('RabbitMQ reporting for method ', $name,
                         ' on data object ', $object);
            my $body = $self->object_message_body($object);
            my $user = $self->get_irods_user;
            my $headers = $self->message_headers($body, $name, $now, $user);
            $self->publish_rmq_message($body, $headers);
        }
        return $object;
    };

}

before 'remove_collection' => sub {
    my ($self, @args) = @_;
    if ($self->enable_rmq) {
        my $collection = $self->ensure_collection_path($args[0]);
        my $body = $self->collection_message_body($collection);
        my $headers = $self->message_headers($body,
                                             'remove_collection',
                                             $self->rmq_timestamp(),
                                             $self->get_irods_user);
        $self->publish_rmq_message($body, $headers);
    }
};

before 'remove_object' => sub {
    my ($self, @args) = @_;
    if ($self->enable_rmq) {
        my $object = $self->ensure_object_path($args[0]);
        $self->debug('RabbitMQ reporting for method remove_object',
                     ' on data object ', $object);
        my $body = $self->object_message_body($object);
        my $headers = $self->message_headers($body,
                                             'remove_object',
                                             $self->rmq_timestamp(),
                                             $self->get_irods_user);
        $self->publish_rmq_message($body, $headers);
    }
};

no Moose::Role;

1;


__END__

=head1 NAME

WTSI::NPG::iRODS::Reportable::iRODSMQ

=head1 DESCRIPTION

A Role to enable reporting of method calls on an iRODS object to a
RabbitMQ message server.

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
