package WTSI::NPG::iRODS::PublisherFactory;

use strict;
use warnings;
use Moose::Role;

use WTSI::NPG::iRODS::Publisher;

with 'WTSI::DNAP::Utilities::Loggable';

our $VERSION = '';

has 'enable_rmq' =>
    (is       => 'ro',
     isa      => 'Bool',
     lazy     => 1,
     default  => 0,
     documentation => 'If true, publish messages to the RabbitMQ '.
         'server. False by default.',
 );

has 'exchange' =>
    (is       => 'ro',
     isa      => 'Maybe[Str]',
     documentation => 'A RabbitMQ exchange name. Relevant only if '.
         'enable_rmq is True.',
);

has 'routing_key_prefix' =>
    (is       => 'ro',
     isa      => 'Maybe[Str]',
     documentation => 'Prefix for the RabbitMQ routing key. May be '.
         'used to distinguish test from production messages. Relevant '.
         'only if enable_rmq is True.',
);



=head2 make_publisher

  Args [n]   : Arguments for creation of the Publisher object.

  Example    : my $publisher = $factory->make_publisher(@args);

  Description: Factory for creating Publisher objects of an appropriate
               class, depending if RabbitMQ messaging is enabled.

               The RabbitMQ parameters 'exchange' and 'routing_key_prefix'
               are specified by attributes of the PublisherFactory Role;
               they must not be included in the list of arguments input to
               this method.

  Returntype : WTSI::NPG::iRODS::Publisher or
               WTSI::NPG::iRODS::PublisherWithReporting

=cut

sub make_publisher {
    my ($self, @args) = @_;
    @args = $self->_process_args(@args);
    my $publisher;
    if ($self->enable_rmq) {
        # 'require' ensures PublisherWithReporting not used unless wanted
        # eg. prerequisite module Net::AMQP::RabbitMQ may not be installed
        require WTSI::NPG::iRODS::PublisherWithReporting;
        $publisher = WTSI::NPG::iRODS::PublisherWithReporting->new(@args);
    } else {
        $publisher = WTSI::NPG::iRODS::Publisher->new(@args);
    }
    return $publisher;
}


# check and update publisher creation arguments
# - if exchange or routing_key_prefix is defined in arguments, croak
# - if RabbitMQ is enabled:
#   - if exchange or routing_key_prefix attribute is defined, populate
#   argument from attribute
#   - otherwise, do not populate argument (Publisher default will be used)

sub _process_args {
    my ($self, %args) = @_;
    my $exchange_key = 'exchange';
    my $prefix_key = 'routing_key_prefix';
    my @rmq_keys = ($exchange_key, $prefix_key);
    foreach my $key (@rmq_keys) {
        if (defined $args{$key}) {
            $self->logcroak
                ('Key/value pair for ', $key, ' must not be defined ',
                 'in Publisher arguments; instead, may be defined in ',
                 'attribute of WTSI::NPG::iRODS::PublisherFactory.');
        }
    }
    if ($self->enable_rmq) {
        if (defined $self->exchange) {
            $args{$exchange_key} = $self->exchange;
        }
        if (defined $self->routing_key_prefix) {
            $args{$prefix_key} = $self->routing_key_prefix;
        }
    }
    return %args;
}


no Moose::Role;

1;



__END__

=head1 NAME

WTSI::NPG::iRODS::PublisherFactory

=head1 DESCRIPTION

A Role for creating Publisher objects of an appropriate class:

=over

=item

WTSI::NPG::iRODS::PublisherWithReporting if RabbitMQ is enabled;

=item

WTSI::NPG::iRODS::Publisher otherwise.

=back


RabbitMQ is enabled if the attribute enable_rmq is true; disabled otherwise.

This role also includes attributes which may be used to store RabbitMQ
parameters: exchange and routing_key_prefix.

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
