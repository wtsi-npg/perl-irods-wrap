package WTSI::NPG::DriRODS;

use namespace::autoclean;
use Data::Dump qw(pp);
use Moose;

our $VERSION = '';

use WTSI::NPG::iRODS::Types qw(:all);

extends 'WTSI::NPG::iRODS';

# These methods are replaced with logging-only alternatives.
my @dry_run_methods = qw(
                          add_collection
                          add_collection_avu
                          add_group
                          add_object
                          add_object_avu
                          calculate_checksum
                          copy_object
                          move_collection
                          move_object
                          put_collection
                          remove_collection
                          remove_collection_avu
                          remove_group
                          remove_object
                          remove_object_avu
                          remove_replicate
                          replace_object
                          set_collection_permissions
                          set_object_permissions
);

foreach my $method (@dry_run_methods) {
  around $method => sub {
    my ($orig, $self, @args) = @_;

    $self->info("Called $method with ", pp(\@args));

    return 1;
  }
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::DriRODS - A drop in replacement for WTSI::NPG::iRODS that
replaces all it data- and metadata-changing methods with logging
stubs.

=head1 DESCRIPTION

This class enables dry-run operations to be carried on iRODS out more
easily. Simply replace your iRODS handle with an instance of this
class and all the method calls that would change data and/or metadata
will be logged at INFO level, along with their arguments.

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
