
package WTSI::NPG::iRODS::DataObjectReader;

use Moose;

extends 'WTSI::NPG::iRODS::Communicator';

has '+executable' => (default => 'baton-get');

around [qw(read_object)] => sub {
  my ($orig, $self, @args) = @_;

  unless ($self->started) {
    $self->logconfess('Attempted to use a WTSI::NPG::iRODS::DataObjectReader ',
                      'without starting it');
  }

  return $self->$orig(@args);
};


=head2 read_object

  Arg [1]    : Data object absoloute path

  Example    : $reader->read_object('/path/to/object.txt')
  Description: Read UTF-8 content from a data object.
  Returntype : Str

=cut

sub read_object {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object =~ m{^/} or
    $self->logconfess("An absolute object path argument is required: ",
                      "received '$object'");

  my ($volume, $collection, $data_name) = File::Spec->splitpath($object);
  $collection = File::Spec->canonpath($collection);

  my $spec = {collection  => $collection,
              data_object => $data_name};

  my $response = $self->communicate($spec);

  $self->validate_response($response);
  $self->report_error($response);

  if (!exists $response->{data}) {
    $self->logconfess('The returned path spec did not have a "data" key: ',
                      $self->encode($response));
  }

  return $response->{data};
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::DataObjectReader

=head1 DESCRIPTION

A client that returns UTF-8 text (only) from iRODS data objects.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (c) 2014 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
