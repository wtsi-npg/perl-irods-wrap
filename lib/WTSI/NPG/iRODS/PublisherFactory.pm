package WTSI::NPG::iRODS::PublisherFactory;

use namespace::autoclean;
use Moose;

use WTSI::NPG::iRODS::Publisher;

our $VERSION = '';

=head2 make_publisher

  Args [n]   : Arguments for creation of the Publisher object.

  Example    : my $publisher = $factory->make_publisher(@args);

  Description: Factory for creating Publisher objects of an appropriate
               class, depending if RabbitMQ messaging is enabled.

  Returntype : WTSI::NPG::iRODS::Publisher or
               WTSI::NPG::iRODS::PublisherWithReporting

=cut

sub make_publisher {
    my ($self, @args) = @_;
    my $publisher;
    if ($ENV{NPG_RMQ_CONFIG}) {
        # 'require' ensures PublisherWithReporting not used unless wanted
        # eg. prerequisite module Net::AMQP::RabbitMQ may not be installed
        require WTSI::NPG::iRODS::PublisherWithReporting;
        $publisher = WTSI::NPG::iRODS::PublisherWithReporting->new(@args);
    } else {
        $publisher = WTSI::NPG::iRODS::Publisher->new(@args);
    }
    return $publisher;
}



__PACKAGE__->meta->make_immutable;

no Moose;

1;



__END__

=head1 NAME

WTSI::NPG::iRODS::PublisherFactory

=head1 DESCRIPTION

A factory for creating Publisher objects of an appropriate class:

=over

=item

WTSI::NPG::iRODS::PublisherWithReporting if RabbitMQ is enabled;

=item

WTSI::NPG::iRODS::Publisher otherwise.

=back

RabbitMQ is enabled if the environment variable NPG_RMQ_CONFIG is set
to a true value; disabled otherwise.


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
