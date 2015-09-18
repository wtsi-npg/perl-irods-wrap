
package WTSI::NPG::iRODS::Annotation;

use strict;
use warnings;
use Moose::Role;

use WTSI::NPG::iRODS::Metadata qw(
                                   $DCTERMS_AUDIENCE
                                   $DCTERMS_CREATED
                                   $DCTERMS_CREATOR
                                   $DCTERMS_IDENTIFIER
                                   $DCTERMS_MODIFIED
                                   $DCTERMS_PUBLISHER
                                   $DCTERMS_TITLE
                                   $FILE_MD5
                                   $FILE_TYPE
                                   $REFERENCE_GENOME_NAME
                                   $RT_TICKET
                                   $SAMPLE_ACCESSION_NUMBER
                                   $SAMPLE_COHORT
                                   $SAMPLE_COMMON_NAME
                                   $SAMPLE_CONSENT
                                   $SAMPLE_CONSENT_WITHDRAWN
                                   $SAMPLE_CONTROL
                                   $SAMPLE_DONOR_ID
                                   $SAMPLE_ID
                                   $SAMPLE_NAME
                                   $SAMPLE_PUBLIC_NAME
                                   $SAMPLE_SUPPLIER_NAME
                                   $STUDY_ACCESSION_NUMBER
                                   $STUDY_ID
                                   $STUDY_NAME
                                   $STUDY_TITLE
                                );

our $VERSION = '';

has 'metadata_attributes' =>
  (is            => 'ro',
   isa           => 'HashRef[Str]',
   required      => 1,
   default       => sub {
     return {
             $DCTERMS_AUDIENCE         => 'dcterms:audience',
             $DCTERMS_CREATED          => 'dcterms:created',
             $DCTERMS_CREATOR          => 'dcterms:creator',
             $DCTERMS_IDENTIFIER       => 'dcterms:identifier',
             $DCTERMS_MODIFIED         => 'dcterms:modified',
             $DCTERMS_PUBLISHER        => 'dcterms:publisher',
             $DCTERMS_TITLE            => 'dcterms:title',
             $FILE_MD5                 => 'md5',
             $FILE_TYPE                => 'type',
             $REFERENCE_GENOME_NAME    => 'reference_name',
             $RT_TICKET                => 'rt_ticket',
             $SAMPLE_ACCESSION_NUMBER  => 'sample_accession_number',
             $SAMPLE_COHORT            => 'sample_cohort',
             $SAMPLE_COMMON_NAME       => 'sample_common_name',
             $SAMPLE_CONSENT           => 'sample_consent',
             $SAMPLE_CONSENT_WITHDRAWN => 'sample_consent_withdrawn',
             $SAMPLE_CONTROL           => 'sample_control',
             $SAMPLE_DONOR_ID          => 'sample_donor_id',
             $SAMPLE_ID                => 'sample_id',
             $SAMPLE_NAME              => 'sample',
             $SAMPLE_PUBLIC_NAME       => 'sample_public_name',
             $SAMPLE_SUPPLIER_NAME     => 'sample_supplier_name',
             $STUDY_ACCESSION_NUMBER   => 'study_accession_number',
             $STUDY_ID                 => 'study_id',
             $STUDY_NAME               => 'study',
             $STUDY_TITLE              => 'study_title'
            }
   },
   documentation => 'A mapping of metadata name to the attribute under ' .
                    'which that metadata is stored.');

=head2 metadata_names

  Arg [1]    : None.

  Example    : my @valid_names = $obj->metadata_names;
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

  Example    : $obj->is_metadata_attr($WTSI::NPG::iRODS::Metadata::SAMPLE_ID);
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

  Example    : my $attr = $obj->metadata_attr
                   ($WTSI::NPG::iRODS::Metadata::SAMPLE_ID);
  Description: Return the metadata attribute under which the named
               data are stored. The name and attribute may be the
               same. However, the default behaviour for the names
               to be those exported from WTSI::NPG::iRODS::Metadata.
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
