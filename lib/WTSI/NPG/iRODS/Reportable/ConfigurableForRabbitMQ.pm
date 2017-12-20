package WTSI::NPG::iRODS::Reportable::ConfigurableForRabbitMQ;

use strict;
use warnings;
use Moose::Role;

our $VERSION = '';

has 'channel' =>
    (is       => 'ro',
     isa      => 'Int',
     default  => 1,
     documentation => 'A RabbitMQ channel. Relevant only if '.
         'enable_rmq is True.',
);

has 'exchange' =>
    (is       => 'ro',
     isa      => 'Str',
     default  => 'npg.gateway',
     documentation => 'A RabbitMQ exchange name. Relevant only if '.
         'enable_rmq is True.',
);

has 'routing_key_prefix' =>
    (is       => 'ro',
     isa      => 'Str',
     default  => 'prod',
     documentation => 'Prefix for the RabbitMQ routing key. May be '.
         'used to distinguish test from production messages. Relevant '.
         'only if enable_rmq is True.',
);

has 'enable_rmq' =>
    (is       => 'ro',
     isa      => 'Bool',
     lazy     => 1,
     default  => 0,
     documentation => 'If true, publish messages to the RabbitMQ '.
         'server. False by default.',
 );


no Moose::Role;

1;


__END__

=head1 NAME

WTSI::NPG::iRODS::Reportable::ConfigurableForRabbitMQ

=head1 DESCRIPTION

A Role providing attributes to configure RabbitMQ reporting.

RabbitMQ is enabled if the attribute enable_rmq is true; disabled otherwise.

Other attributes may be used to store RabbitMQ parameters: channel,
exchange and routing_key_prefix.

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
