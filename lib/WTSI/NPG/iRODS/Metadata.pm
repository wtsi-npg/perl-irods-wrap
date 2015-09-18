
package WTSI::NPG::iRODS::Metadata;

use strict;
use warnings;
use Exporter qw(import);

our @EXPORT_OK = qw(
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

our $DCTERMS_AUDIENCE         = __PACKAGE__ . '::dcterms_audience';
our $DCTERMS_CREATED          = __PACKAGE__ . '::dcterms_created';
our $DCTERMS_CREATOR          = __PACKAGE__ . '::dcterms_creator';
our $DCTERMS_IDENTIFIER       = __PACKAGE__ . '::dcterms_identifier';
our $DCTERMS_MODIFIED         = __PACKAGE__ . '::dcterms_modified';
our $DCTERMS_PUBLISHER        = __PACKAGE__ . '::dcterms_publisher';
our $DCTERMS_TITLE            = __PACKAGE__ . '::dcterms_title';
our $FILE_MD5                 = __PACKAGE__ . '::file_md5';
our $FILE_TYPE                = __PACKAGE__ . '::file_type';
our $REFERENCE_GENOME_NAME    = __PACKAGE__ . '::reference_genome_name';
our $RT_TICKET                = __PACKAGE__ . '::rt_ticket';
our $SAMPLE_ACCESSION_NUMBER  = __PACKAGE__ . '::sample_accession_number';
our $SAMPLE_COHORT            = __PACKAGE__ . '::sample_cohort';
our $SAMPLE_COMMON_NAME       = __PACKAGE__ . '::sample_common_name';
our $SAMPLE_CONSENT           = __PACKAGE__ . '::sample_consent';
our $SAMPLE_CONSENT_WITHDRAWN = __PACKAGE__ . '::sample_consent_withdrawn';
our $SAMPLE_CONTROL           = __PACKAGE__ . '::sample_control';
our $SAMPLE_DONOR_ID          = __PACKAGE__ . '::sample_donor_id';
our $SAMPLE_ID                = __PACKAGE__ . '::sample_id';
our $SAMPLE_NAME              = __PACKAGE__ . '::sample_name';
our $SAMPLE_PUBLIC_NAME       = __PACKAGE__ . '::sample_public_name';
our $SAMPLE_SUPPLIER_NAME     = __PACKAGE__ . '::sample_supplier_name';
our $STUDY_ACCESSION_NUMBER   = __PACKAGE__ . '::study_accession_number';
our $STUDY_ID                 = __PACKAGE__ . '::study_id';
our $STUDY_NAME               = __PACKAGE__ . '::study_name';
our $STUDY_TITLE              = __PACKAGE__ . '::study_title';

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::Metadata

=head1 DESCRIPTION

This package exports symbols for describing metadata. These are not
arbitrary strings used internally by the WTSI::NPG::iRODS code.

See WTSI::NPG::iRODS::Annotation where these symbols are mapped to the
actual attribute strings used in iRODS.

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
