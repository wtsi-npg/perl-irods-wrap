package WTSI::NPG::iRODS::PublisherWithReporting;

use Moose;

our $VERSION = '';

extends 'WTSI::NPG::iRODS::Publisher';

with 'WTSI::NPG::iRODS::Reportable::PublisherMQ';

sub BUILD {
    my ($self, ) = @_;
    return $self->rmq_init();
}

sub DEMOLISH {
    my ($self, ) = @_;
    return $self->rmq_disconnect();
}


__PACKAGE__->meta->make_immutable;

no Moose;

1;



__END__

=head1 NAME

WTSI::NPG::iRODS::PublisherWithReporting

=head1 DESCRIPTION

Subclass of WTSI::NPG::iRODS::Publisher. Reports specified message calls
by sending messages to a RabbitMQ server. Automatically connects to
and disconnects from the RabbitMQ server on object creation and destruction,
respectively.

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
