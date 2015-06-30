
package WTSI::NPG::iRODS::Communicator;

use Moose;
use Try::Tiny;

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Startable', 'WTSI::DNAP::Utilities::JSONCodec';

sub communicate {
  my ($self, $spec) = @_;

  my $json = $self->encode($spec);
  ${$self->stdin} .= $json;
  ${$self->stderr} = q{};

  $self->debug("Sending JSON spec $json to ", $self->executable);

  my $response;

  try {
    # baton sends JSON responses on a single line
    $self->harness->pump until ${$self->stdout} =~ m{[\r\n]$}msx;
    $response = $self->decode(${$self->stdout});
    ${$self->stdout} = q{};
  } catch {
    $self->error("JSON parse error on: '", ${$self->stdout}, "': ", $_);
  };

  defined $response or
    $self->logconfess("Failed to get a response from JSON spec '$json'");

  $self->debug("Got a response of ", $self->encode($response));

  return $response;
}

sub validate_response {
  my ($self, $response) = @_;

  # The ony valid response is a HashRef
  my $rtype = ref $response;
  unless ($rtype eq 'HASH') {
    $self->logconfess("Failed to get a HashRef response; got $rtype");
  }

  return $self;
}

sub report_error {
  my ($self, $response) = @_;

  if (ref $response eq 'HASH' and exists $response->{error}) {
    $self->logconfess($response->{error}->{message}, " Error code: ",
                      $response->{error}->{code});
  }

  return $self;
}

sub path_spec_str {
  my ($self, $path_spec) = @_;

  defined $path_spec or
    $self->logconfess('A defined path_spec argument is required');

  ref $path_spec eq 'HASH' or
    $self->logconfess('A HashRef path_spec argument is required');

  exists $path_spec->{collection} or
    $self->logconfess('The path_spec argument did not have a "collection" key');

  my $path = $path_spec->{collection};
  if (exists $path_spec->{data_object}) {
    $path = $path . q{/} . $path_spec->{data_object};
  }

  return $path;
}

sub path_spec_checksum {
  my ($self, $path_spec) = @_;

    defined $path_spec or
    $self->logconfess('A defined path_spec argument is required');

  ref $path_spec eq 'HASH' or
    $self->logconfess('A HashRef path_spec argument is required');

  exists $path_spec->{checksum} or
    $self->logconfess('The path_spec argument did not have a "checksum" key');

  return $path_spec->{checksum};
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::Communicator

=head1 DESCRIPTION

A client that lists iRODS metadata as JSON.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2014, 2015 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
