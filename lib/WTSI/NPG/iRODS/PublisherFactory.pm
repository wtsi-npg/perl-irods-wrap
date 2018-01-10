package WTSI::NPG::iRODS::PublisherFactory;

use strict;
use warnings;
use Moose;

use WTSI::NPG::iRODS::Publisher;

with qw [WTSI::NPG::iRODS::Reportable::ConfigurableForRabbitMQ
         WTSI::DNAP::Utilities::Loggable
    ];

our $VERSION = '';

has 'irods' =>
  (isa           => 'WTSI::NPG::iRODS',
   is            => 'ro',
   required      => 1,
   documentation => 'An iRODS connection handle for publication');

has 'checksum_cache_threshold' =>
  (is            => 'ro',
   isa           => 'Int',
   required      => 1,
   default       => 2048,
   documentation => 'The size above which file checksums will be cached');

has 'require_checksum_cache' =>
  (is            => 'ro',
   isa           => 'ArrayRef[Str]',
   required      => 1,
   default       => sub { return [qw[bam cram]] },
   documentation => 'A list of file suffixes for which MD5 cache files ' .
                    'must be provided and will not be created on the fly');

has 'checksum_cache_time_delta' =>
  (is            => 'rw',
   isa           => 'Int',
   required      => 1,
   default       => 60,
   documentation => 'Time delta in seconds for checksum cache files to be ' .
                    'considered stale. If a data file is newer than its '   .
                    'cache by more than this number of seconds, the cache ' .
                    'is stale');


=head2 make_publisher

  Args [n]   : Arguments for creation of the Publisher object.

  Example    : my $publisher = $factory->make_publisher();

  Description: Factory for creating Publisher objects of an appropriate
               class, depending if RabbitMQ messaging is enabled. Arguments
               for Publisher construction are derived from class attributes.

  Returntype : WTSI::NPG::iRODS::Publisher or
               WTSI::NPG::iRODS::PublisherWithReporting

=cut

sub make_publisher {
    my ($self, ) = @_;
    my @args;
    if ($self->enable_rmq) {
        push @args, 'enable_rmq'         => 1;
        push @args, 'channel'            => $self->channel;
        push @args, 'exchange'           => $self->exchange;
        push @args, 'routing_key_prefix' => $self->routing_key_prefix;
    }
    push @args, 'irods'                    => $self->irods;
    push @args, 'checksum_cache_threshold' => $self->checksum_cache_threshold;
    push @args, 'require_checksum_cache'   => $self->require_checksum_cache;
    push @args,
        'checksum_cache_time_delta' => $self->checksum_cache_time_delta;
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


no Moose;

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

<<<<<<< HEAD
This role also includes attributes which may be used to store RabbitMQ
parameters: exchange and routing_key_prefix.
=======
>>>>>>> rmq_publisher_devel

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
