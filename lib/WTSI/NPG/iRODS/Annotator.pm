package WTSI::NPG::iRODS::Annotator;

use DateTime;
use List::AllUtils qw[uniq];
use Moose::Role;

use WTSI::NPG::iRODS::Metadata;

our $VERSION = '';

our @COMPRESSION_SUFFIXES     = qw[bz2 gz xz zip];

our @GENERAL_PURPOSE_SUFFIXES = qw[bin csv h5 tar tgz tif tsv txt xls xlsx xml];
our @GENO_DATA_SUFFIXES       = qw[gtc idat];
our @HTS_DATA_SUFFIXES        = qw[bam cram bai crai];
our @HTS_ANCILLARY_SUFFIXES   = qw[bamcheck bed flagstat json seqchksum
                                   stats xml];

our @DEFAULT_FILE_SUFFIXES = (@GENERAL_PURPOSE_SUFFIXES,
                              @GENO_DATA_SUFFIXES,
                              @HTS_DATA_SUFFIXES,
                              @HTS_ANCILLARY_SUFFIXES);

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::iRODS::Utilities
       ];

my $COMPRESSION_PATTERN = join q[|], @COMPRESSION_SUFFIXES;

# See http://dublincore.org/documents/dcmi-terms/

=head2 make_creation_metadata

  Arg [1]    : Creating person, organization, or service, URI.
  Arg [2]    : Creation time, DateTime
  Arg [3]    : Publishing person, organization, or service, URI.

  Example    : my @avus = $ann->make_creation_metadata($time, $publisher)
  Description: Return a list of metadata AVUs describing the creation of
               an item.
  Returntype : Array[HashRef]

=cut

sub make_creation_metadata {
  my ($self, $creator, $creation_time, $publisher) = @_;

  defined $creator or
    $self->logconfess('A defined creator argument is required');
  defined $creation_time or
    $self->logconfess('A defined creation_time argument is required');
  defined $publisher or
    $self->logconfess('A defined publisher argument is required');

  return
    ($self->make_avu($DCTERMS_CREATOR,   $creator->as_string),
     $self->make_avu($DCTERMS_CREATED,   $creation_time->iso8601),
     $self->make_avu($DCTERMS_PUBLISHER, $publisher->as_string));
}

=head2 make_modification_metadata

  Arg [1]    : Modification time, DateTime.

  Example    : my @avus = $ann->make_modification_metadata($time)
  Description: Return an array of of metadata AVUs describing the
               modification of an item.
  Returntype : Array[HashRef]

=cut

sub make_modification_metadata {
  my ($self, $modification_time) = @_;

  defined $modification_time or
    $self->logconfess('A defined modification_time argument is required');

  return ($self->make_avu($DCTERMS_MODIFIED, $modification_time->iso8601));
}

=head2 make_type_metadata

  Arg [1]    : File name, Str.
  Arg [2]    : Array of valid file suffix strings, Str. Optional

  Example    : my @avus = $ann->make_type_metadata($sample, 'txt', 'csv')
  Description: Return an array of metadata AVUs describing the file 'type'
               (represented by its suffix).
  Returntype : Array[HashRef]

=cut

sub make_type_metadata {
  my ($self, $file, @suffixes) = @_;

  defined $file or $self->logconfess('A defined file argument is required');
  $file eq q[] and $self->logconfess('A non-empty file argument is required');

  my @valid_suffixes = uniq (@DEFAULT_FILE_SUFFIXES, @suffixes);

  my $suffix_pattern = join q[|], @valid_suffixes;
  my $suffix_regex = qr{[.]  # Don't capture the suffix dot
                        (
                          ($suffix_pattern)
                          ([.]($COMPRESSION_PATTERN))*
                        )$}msx;
  my ($suffix) = $file =~ $suffix_regex;

  my @avus;
  if ($suffix) {
    $self->debug("Parsed base suffix of '$file' as '$suffix'");
    push @avus, $self->make_avu($FILE_TYPE, $suffix);
  }
  else {
    $self->debug("Did not parse a suffix from '$file'");
  }

  return @avus;
}

=head2 make_md5_metadata

  Arg [1]    : Checksum, Str.

  Example    : my @avus = $ann->make_md5_metadata($checksum)
  Description: Return an array of metadata AVUs describing the
               file MD5 checksum.
  Returntype : Array[HashRef]

=cut

sub make_md5_metadata {
  my ($self, $md5) = @_;

  defined $md5 or $self->logconfess('A defined md5 argument is required');
  $md5 eq q[] and $self->logconfess('A non-empty md5 argument is required');

  return ($self->make_avu($FILE_MD5, $md5));
}

=head2 make_sample_metadata

  Arg [1]    : Array[WTSI::DNAP::Warehouse::Schema::Result::Sample]
  Example    : my @avus = $ann->make_sample_metadata(@samples);
  Description: Return HTS sample metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_sample_metadata {
  my ($self, @samples) = @_;
  my $method_attr = {accession_number => $SAMPLE_ACCESSION_NUMBER,
                     id_sample_lims   => $SAMPLE_ID,
                     name             => $SAMPLE_NAME,
                     public_name      => $SAMPLE_PUBLIC_NAME,
                     common_name      => $SAMPLE_COMMON_NAME,
                     supplier_name    => $SAMPLE_SUPPLIER_NAME,
                     cohort           => $SAMPLE_COHORT,
                     donor_id         => $SAMPLE_DONOR_ID};
  return $self->_make_multi_value_metadata(\@samples, $method_attr);
}

=head2 make_study_metadata

  Arg [1]      Array[WTSI::DNAP::Warehouse::Schema::Result::Study].

  Example    : my @avus = $ann->make_sample_metadata(@studies);
  Description: Return HTS study metadata AVUs.
  Returntype : Array[HashRef]

=cut

sub make_study_metadata {
  my ($self, @studies) = @_;
  my $method_attr = {id_study_lims    => $STUDY_ID,
                     accession_number => $STUDY_ACCESSION_NUMBER,
                     name             => $STUDY_NAME,
                     study_title      => $STUDY_TITLE};
  return $self->_make_multi_value_metadata(\@studies, $method_attr);
}

=head2 make_ticket_metadata

  Arg [1]    : string filename

  Example    : my @avus = $ann->make_ticket_metadata($ticket_number)
  Description: Return an array of metadata AVUs describing an RT ticket
               relating to the file.
  Returntype : Array[HashRef]

=cut

sub make_ticket_metadata {
  my ($self, $ticket_number) = @_;

  defined $ticket_number or
    $self->logconfess('A defined ticket_number argument is required');
  $ticket_number eq q[] and
    $self->logconfess('A non-empty ticket_number argument is required');

  return ($self->make_avu($RT_TICKET, $ticket_number));
}

sub _make_multi_value_metadata {
  my ($self, $objs, $method_attr) = @_;
  # The method_attr argument is a map of method name to attribute name
  # under which the result will be stored.

  my @avus;
  foreach my $method_name (sort keys %{$method_attr}) {
    my $attr = $method_attr->{$method_name};
    foreach my $obj (@{$objs}) {
      my $value = $obj->$method_name;
      if (defined $value) {
        $self->debug($obj, "::$method_name returned ", $value);
        push @avus, $self->make_avu($attr, $value);
      }
      else {
        $self->debug($obj, "::$method_name returned undef");
      }
    }
  }

  return @avus;
}

no Moose::Role;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::Annotator

=head1 DESCRIPTION

A role providing methods to calculate metadata for data published to
iRODS.  Please prefer the API for creating metadata, rather than
creating it using string literals in your own package.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>, Iain Bancarz <ib5@sanger.ac.uk>

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
