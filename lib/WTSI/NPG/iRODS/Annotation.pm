
package WTSI::NPG::iRODS::Annotation;

use strict;
use warnings;
use Moose::Role;

our $VERSION = '';


has 'metadata_attributes' =>
  (is            => 'ro',
   isa           => 'HashRef[Str]',
   required      => 1,
   default       => sub {
     return {
             dcterms_audience        => 'dcterms:audience',
             dcterms_created         => 'dcterms:created',
             dcterms_creator         => 'dcterms:creator',
             dcterms_identifier      => 'dcterms:identifier',
             dcterms_modified        => 'dcterms:modified',
             dcterms_publisher       => 'dcterms:publisher',
             dcterms_title           => 'dcterms:title',
             file_md5                => 'md5',
             file_type               => 'type',
             reference_genome_name   => 'reference_name',
             rt_ticket               => 'rt_ticket',
             sample_accession_number => 'sample_accession_number',
             sample_cohort           => 'sample_cohort',
             sample_common_name      => 'sample_common_name',
             sample_control          => 'sample_control',
             sample_donor_id         => 'sample_donor_id',
             sample_id               => 'sample_id',
             sample_name             => 'sample',
             sample_supplier_name    => 'sample_supplier_name',
             study_id                => 'study_id',
             study_id                => 'study_id',
             study_title             => 'study_title'
            }
   },
   documentation => 'A mapping of metadata name to the attribute under ' .
                    'which that metadata is stored.');

=head2 metadata_names

  Arg [1]    : None.

  Example    : my @valid_names  = $obj->metadata_names;
  Description: Returns a sorted list of all the valis metadata names.
  Returntype : Array[Str].

=cut

sub metadata_names {
  my ($self) = @_;

  my @names = keys $self->metadata_attributes;
  @names = sort @names;

  return @names;
}

=head2 is_metadata_attr

  Arg [1]    : Name, Str.

  Example    : $obj->is_metadata_attr('sample_name');
  Description: Returns true if the argument names a valid type of metadata
               attribute.
  Returntype : Bool.

=cut

sub is_metadata_attr {
  my ($self, $name) = @_;

  return exists $self->metadata_attributes->{$name};
}

=head2 metadata_attr

  Arg [1]    : Name, Str

  Example    : my $attr = $obj->metadata_attr('sample_name');
  Description: Return the metadata attribute under which the named
               data are stored. The name and attribute may be the
               same. However, sometimes the attribute is different e.g.
               the 'sample_name' has historically been stored under
               the 'sample' attribute.
  Returntype : Str

=cut

sub metadata_attr {
  my ($self, $name) = @_;

  if (not $self->is_metadata_attr($name)) {
    $self->logconfess("There is no metadata attribute to store a '$name'");
  }

  return $self->metadata_attributes->{$name};
}

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::Annotation - Metadata attributes.

=head1 DESCRIPTION

Provides methods to access metadata attributes.

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
