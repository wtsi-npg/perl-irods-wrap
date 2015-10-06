package WTSI::NPG::iRODS::Types;

use strict;
use warnings;

use MooseX::Types::Moose qw(ArrayRef Bool Int Str);

use MooseX::Types -declare =>
  [
   qw(
       AbsolutePath
       ArrayRefOfReplicate
       BoolInt
       Collection
       CoreMetadataAttr
       DataObject
       GTYMetadataAttr
       HTSMetadataAttr
       JSONBool
       NoWhitespaceStr
       Replicate
     )
  ];

use WTSI::NPG::iRODS::Metadata; # Imports all exported symbols.

our $VERSION = '';

our @CORE_METADATA_ATTRS = (
                            $DCTERMS_AUDIENCE,
                            $DCTERMS_CREATED,
                            $DCTERMS_CREATOR,
                            $DCTERMS_IDENTIFIER,
                            $DCTERMS_MODIFIED,
                            $DCTERMS_PUBLISHER,
                            $DCTERMS_TITLE,

                            $FILE_MD5,
                            $FILE_TYPE,
                            $QC_STATE,
                            $RT_TICKET,

                            $SAMPLE_ACCESSION_NUMBER,
                            $SAMPLE_COHORT,
                            $SAMPLE_COMMON_NAME,
                            $SAMPLE_CONSENT,
                            $SAMPLE_CONSENT_WITHDRAWN,
                            $SAMPLE_CONTROL,
                            $SAMPLE_DONOR_ID,
                            $SAMPLE_ID,
                            $SAMPLE_NAME,
                            $SAMPLE_PUBLIC_NAME,
                            $SAMPLE_SUPPLIER_NAME,
                            $STUDY_ACCESSION_NUMBER,
                            $STUDY_ID,
                            $STUDY_NAME,
                            $STUDY_TITLE,
                           );

our @HTS_METADATA_ATTRS = (
                           $ALIGNMENT,
                           $CONTROL,
                           $ID_RUN,
                           $IS_PAIRED_READ,
                           $LIBRARY,
                           $LIBRARY_ID,
                           $POSITION,
                           $QC_STATE,
                           $REFERENCE,
                           $TAG,
                           $TAG_INDEX,
                           $TARGET,
                           $TOTAL_READS,
                          );

our @GTY_METADATA_ATTRS = (
                           $ANALYSIS_UUID,
                           $INFINIUM_PROJECT_TITLE,
                           $INFINIUM_BEADCHIP,
                           $INFINIUM_BEADCHIP_DESIGN,
                           $INFINIUM_BEADCHIP_SECTION,
                           $INFINIUM_PLATE_NAME,
                           $INFINIUM_PLATE_WELL,
                           $INFINIUM_SAMPLE_NAME,
                           $SEQUENOM_PLATE_NAME,
                           $SEQUENOM_PLATE_WELL,
                           $SEQUENOM_PLEX_NAME,
                           $FLUIDIGM_PLATE_NAME,
                           $FLUIDIGM_PLATE_WELL,
                           $FLUIDIGM_PLEX_NAME,
                          );

my $CORE_METADATA_INDEX = { map { $_ => 1 } @CORE_METADATA_ATTRS };
my $HTS_METADATA_INDEX  = { map { $_ => 1 } @CORE_METADATA_ATTRS,
                            @HTS_METADATA_ATTRS  };
my $GTY_METADATA_INDEX = { map { $_ => 1 } @CORE_METADATA_ATTRS,
                           @GTY_METADATA_ATTRS  };

subtype AbsolutePath,
  as Str,
  where { m{^/}msx },
  message { "'$_' is not an absolute path" };

subtype NoWhitespaceStr,
  as Str,
  where { m{^\S+$}msx },
  message { "'$_' is a string containing whitespace" };

subtype CoreMetadataAttr,
  as Str,
  where { exists $CORE_METADATA_INDEX->{$_} },
  message { "'$_' is not a valid core metadata attribute" };

subtype HTSMetadataAttr,
  as Str,
  where { exists $HTS_METADATA_INDEX->{$_} },
  message { "'$_' is not a valid HTS metadata attribute" };

subtype GTYMetadataAttr,
  as Str,
  where { exists $GTY_METADATA_INDEX->{$_} },
  message { "'$_' is not a valid HTS metadata attribute" };

class_type DataObject, { class => 'WTSI::NPG::iRODS::DataObject' };

class_type Collection, { class => 'WTSI::NPG::iRODS::Collection' };

class_type Replicate,  { class => 'WTSI::NPG::iRODS::Replicate' };

class_type JSONBool,   { class => 'JSON::XS::Boolean' };

subtype ArrayRefOfReplicate,
  as ArrayRef[Replicate];

subtype BoolInt,
  as Int,
  where { $_ == 0 or $_ == 1 },
  message { "'$_' is not 0 or 1" };

coerce BoolInt,
  from JSONBool,
  via { $_ + 0 };


1;

__END__

=head1 NAME

WTSI::NPG::iRODS::Types - Moose types for iRODS

=head1 DESCRIPTION

The non-core Moose types for iRODS are all defined here.

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
