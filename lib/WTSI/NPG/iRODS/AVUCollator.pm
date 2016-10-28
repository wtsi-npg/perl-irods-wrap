package WTSI::NPG::iRODS::AVUCollator;

use Data::Dump qw[pp];
use Moose::Role;

our $VERSION = '';

with qw[WTSI::DNAP::Utilities::Loggable];

=head2 collate_avus

  Arg [1]    : AVUs to collate, Array[HashRef]

  Example    : my $collated = $self->collate_avus(@avus)
  Description: Collate the values of all the argument AVUs having the same
               attribute into an ArrayRef. Return a HashRef of these
               ArrayRefs where each key is the corresponding attribute.
  Returntype : HashRef[ArrayRef]

=cut

sub collate_avus {
  my ($self, @avus) = @_;

  # Collate into lists of values per attribute
  my %collated_avus;
  foreach my $avu (@avus) {
    my $avu_str = pp($avu);
    if (not ref $avu eq 'HASH') {
      $self->logconfess("Failed to collate AVU $avu_str : it is not a HashRef");
    }
    if (not exists $avu->{attribute}) {
      $self->logconfess("Failed to collate AVU $avu_str : missing attribute");
    }
    if (not exists $avu->{value}) {
      $self->logconfess("Failed to collate AVU $avu_str : missing value");
    }

    my $attr  = $avu->{attribute};
    my $value = $avu->{value};
    if (exists $collated_avus{$attr}) {
      push @{$collated_avus{$attr}}, $value;
    }
    else {
      $collated_avus{$attr} = [$value];
    }
  }

  # foreach my $attr (keys %collated_avus) {
  #   my @values = @{$collated_avus{$attr}};
  #   @values = sort @values;
  #   $collated_avus{$attr} = \@values;
  # }

  $self->debug('Collated ', scalar @avus, ' AVUs into ',
               scalar keys %collated_avus, ' lists');

  return \%collated_avus;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::AVUCollator

=head1 DESCRIPTION

A role providing methods to collate metadata i.e. gather togther all
the values for each distinct attribute in a list of AVUs.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015, 2016 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
