
package WTSI::NPG::iRODS::MetaSearcher;

use File::Spec;
use Moose;

our $VERSION = '';

extends 'WTSI::NPG::iRODS::Communicator';

has '+executable' => (default => 'baton-metaquery');

around [qw(search)] => sub {
  my ($orig, $self, @args) = @_;

  unless ($self->started) {
    $self->logconfess('Attempted to use a WTSI::NPG::iRODS::MetaSearcher ',
                      'without starting it');
  }

  return $self->$orig(@args);
};

sub search {
  my ($self, $zone_hint, @avus) = @_;

   defined $zone_hint or
     $self->logconfess('A defined zone_hint argument is required');

  my $i = 0;
  foreach my $avu (@avus) {
    unless (ref $avu eq 'HASH') {
      $self->logconfess("A query AVU must be a HashRef: AVU #$i was not");
    }
    unless ($avu->{attribute}) {
      $self->logconfess("A query AVU must have an attribute: AVU #$i did not");
    }
    unless ($avu->{value}) {
      $self->logconfess("A query AVU must have a value: AVU #$i did not");
    }
    $i++;
  }

  my $spec = {collection => $zone_hint,
              avus       => \@avus};

  my $response = $self->communicate($spec);
  $self->validate_response($response);
  $self->report_error($response);

  my @results =  map { $self->path_spec_str($_) } @$response;

  return \@results;
}

sub validate_response {
  my ($self, $response) = @_;

  # The ony valid response is a HashRef or ArrayRef
  my $rtype = ref $response;
  unless ($rtype eq 'HASH' || $rtype eq 'ARRAY') {
    $self->logconfess("Failed to get a HashRef or Array response; got $rtype");
  }

  return $self;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::MetaSearcher

=head1 DESCRIPTION

A client that searches iRODS metadata.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2014 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
