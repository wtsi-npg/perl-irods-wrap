package WTSI::NPG::iRODS::Replicate;

use namespace::autoclean;
use Moose;

our $VERSION = '';

has 'number' =>
  (is            => 'ro',
   isa           => 'Int',
   required      => 1,
   documentation => 'The replicate number as reported by iRODS.');

has 'checksum' =>
  (is            => 'ro',
   isa           => 'Str',
   required      => 1,
   documentation => 'The checksum of the replicate.');

has 'resource' =>
  (is             => 'ro',
   isa            => 'Str',
   required       => 1,
   documentation  => 'The name of the iRODS resource hosting the replicate.');

has 'location' =>
  (is            => 'ro',
   isa           => 'Str',
   required      => 1,
   documentation => 'The iRODS location of the replicate.');

has 'valid' =>
  (is            => 'ro',
   isa           => 'Bool',
   required      => 1,
   documentation => 'The state of the replicate as reported by iRODS.');

sub is_valid {
  my ($self) = @_;

  return $self->valid;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::Replicate - A class describing the replication
characteristics of an iRODS data object;

=head1 DESCRIPTION

Describes a single replicate of an iRODS data object in terms of its
replicate number, checksum, resource, location and status.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
