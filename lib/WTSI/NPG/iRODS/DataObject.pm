
package WTSI::NPG::iRODS::DataObject;

use File::Spec;
use List::AllUtils qw(any uniq);
use Moose;
use Set::Scalar;
use Try::Tiny;

use WTSI::NPG::iRODS;

our $VERSION = '';

with 'WTSI::NPG::iRODS::Path';

has 'data_object' =>
  (is        => 'ro',
   isa       => 'Str',
   required  => 1,
   lazy      => 1,
   default   => q{.},
   predicate => 'has_data_object');

has 'checksum' => (is        => 'rw',
                   isa       => 'Str',
                   predicate => 'has_checksum',
                   clearer   => 'clear_checksum');

# TODO: Add a check so that a DataObject cannot be built from a path
# that is in fact a collection.
around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  if (@args == 2 && ref $args[0] eq 'WTSI::NPG::iRODS') {
    my ($volume, $collection, $data_name) = File::Spec->splitpath($args[1]);
    $collection = File::Spec->canonpath($collection);
    $collection ||= q{.};

    return $class->$orig(irods       => $args[0],
                         collection  => $collection,
                         data_object => $data_name);
  }
  else {
    return $class->$orig(@_);
  }
};

# Lazily load metadata from iRODS
around 'metadata' => sub {
  my ($orig, $self) = @_;

  unless ($self->has_metadata) {
    my @meta = $self->irods->get_object_meta($self->str);
    $self->$orig(\@meta);
  }

  return $self->$orig;
};

# Lazily load checksum from iRODS
around 'checksum' => sub {
  my ($orig, $self) = @_;

  unless ($self->has_checksum) {
    my $checksum = $self->irods->checksum($self->str);
    $self->$orig($checksum);
  }

  return $self->$orig;
};

=head2 is_present

  Arg [1]    : None

  Example    : $path->is_present && print $path->str
  Description: Return true if the data object file exists in iRODS.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub is_present {
  my ($self) = @_;

  return $self->irods->list_object($self->str);
}

=head2 absolute

  Arg [1]    : None

  Example    : $path->absolute
  Description: Return the absolute path of the data object.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub absolute {
  my ($self) = @_;

  my $absolute;
  if (File::Spec->file_name_is_absolute($self->str)) {
    $absolute = $self->str;
  }
  else {
    unless ($self->irods) {
      $self->logconfess("Failed to make '", $self->str, "' into an absolute ",
                        "path because it has no iRODS handle attached.");
    }

    $absolute = File::Spec->catfile($self->irods->working_collection,
                                    $self->collection, $self->data_object);
  }

  return WTSI::NPG::iRODS::DataObject->new($self->irods, $absolute);
}

=head2 calculate_checksum

  Arg [1]    : None

  Example    : $obj->calculate_checksum
  Description: Return the MD5 checksum of the data object.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub calculate_checksum {
  my ($self) = @_;

  $self->clear_checksum;
  return $self->irods->calculate_checksum($self->str);
}

=head2 validate_checksum_metadata

  Arg [1]    : None

  Example    : $obj->validate_checksum_metadata
  Description: Return true if the MD5 checksum in the metadata of the
               object is identical to the MD5 calculated by iRODS.
  Returntype : boolean

=cut

sub validate_checksum_metadata {
  my ($self) = @_;

  return $self->irods->validate_checksum_metadata($self->str);
}

=head2 add_avu

  Arg [1]    : Str attribute
  Arg [2]    : Str value
  Arg [2]    : Str units (optional)

  Example    : $path->add_avu('foo', 'bar')
  Description: Add an AVU to an iRODS path (data object or collection)
               Return self. Clear the metadata cache.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub add_avu {
  my ($self, $attribute, $value, $units) = @_;

  if ($self->find_in_metadata($attribute, $value, $units)) {
    my $units_str = defined $units ? "'$units'" : 'undef';

    $self->debug("Failed to add AVU {'$attribute', '$value', $units_str} ",
                 "to '", $self->str, "': AVU is already present");
  }
  else {
    $self->irods->add_object_avu($self->str, $attribute, $value, $units);
  }

  $self->clear_metadata;

  return $self;
}

=head2 remove_avu

  Arg [1]    : Str attribute
  Arg [2]    : Str value
  Arg [2]    : Str units (optional)

  Example    : $path->remove_avu('foo', 'bar')
  Description: Remove an AVU from an iRODS path (data object or collection)
               Return self. Clear the metadata cache.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub remove_avu {
  my ($self, $attribute, $value, $units) = @_;

  if ($self->find_in_metadata($attribute, $value, $units)) {
    $self->irods->remove_object_avu($self->str, $attribute, $value, $units);
  }
  else {
    $self->logconfess("Failed to remove AVU ",
                      "{'$attribute', '$value', '$units'} from '", $self->str,
                      "': AVU is not present");
  }

  $self->clear_metadata;

  return $self;
}

=head2 make_avu_history

  Arg [1]    : Str attribute
  Arg [2]    : DateTime (optional)

  Example    : $obj->make_avu_history('foo')
  Description: Return a history value showing the current state of all
               AVUs with the specified attribute, suitable for adding
               as a history AVU.
  Returntype : Str

=cut

sub make_avu_history {
  my ($self, $attribute, $timestamp) = @_;

  return $self->irods->make_object_avu_history
    ($self->str, $attribute, $timestamp);
}

=head2 get_permissions

  Arg [1]    : None

  Example    : $obj->get_permissions
  Description: Return a list of ACLs defined for the object.
  Returntype : Array

=cut

sub get_permissions {
  my ($self) = @_;

  my $path = $self->str;
  return $self->irods->get_object_permissions($path);
}

=head2 set_permissions

  Arg [1]    : Str permission, one of 'null', 'read', 'write' or 'own'
  Arg [2]    : Array of owners (users and /or groups).

  Example    : $obj->set_permissions('read', 'user1', 'group1')
  Description: Set access permissions on the object. Return self.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub set_permissions {
  my ($self, $permission, @owners) = @_;

  my $perm_str = defined $permission ? $permission : 'null';

  my $path = $self->str;
  foreach my $owner (@owners) {
    $self->info("Giving owner '$owner' '$perm_str' access to '$path'");
    $self->irods->set_object_permissions($perm_str, $owner, $path);
  }

  return $self;
}

=head2 get_groups

  Arg [1]      permission Str, one of 'null', 'read', 'write' or 'own',
               optional.

  Example    : $obj->get_object_groups('read')
  Description: Return a list of the data access groups in the object's ACL.
               If a permission level argument is supplied, only groups with
               that level of access will be returned.
  Returntype : Array

=cut

sub get_groups {
  my ($self, $level) = @_;

  return $self->irods->get_object_groups($self->str, $level);
}

sub update_group_permissions {
  my ($self, $strict_groups) = @_;

  $strict_groups = $strict_groups ? 1 : 0;
  # If strict_groups is true, we only work with groups we can see with
  # igroupadmin. Across zones we usually have to work non-strict
  # because the stock igroupadmin can't see them.

  # Record the current group permissions
  my @groups_permissions = $self->get_groups('read');
  my @groups_annotated = $self->expected_groups;

  $self->debug("Permissions before: [", join(", ", @groups_permissions), "]");
  $self->debug("Updated annotations: [", join(", ", @groups_annotated), "]");

  if ($self->get_avu($self->sample_consent_attr, 0)) {
    $self->info("Data is marked as CONSENT WITHDRAWN; ",
                "all permissions will be withdrawn");
    @groups_annotated = (); # Emptying this means all will be removed
  }

  my $perms = Set::Scalar->new(@groups_permissions);
  my $annot = Set::Scalar->new(@groups_annotated);
  my @to_remove = $perms->difference($annot)->members;
  my @to_add    = $annot->difference($perms)->members;

  $self->debug("Groups to remove: [", join(', ', @to_remove), "]");

  # We try/catch for each group in order to do our best, while
  # counting any errors and failing afterwards if the update was not
  # clean.
  my $num_errors = 0;

  my @all_groups = $self->irods->list_groups;
  foreach my $group (@to_remove) {
    if (not $strict_groups or any { $group eq $_ } @all_groups) {
      try {
        $self->set_permissions('null', $group);
      } catch {
        $num_errors++;
        $self->error("Failed to remove permissions for group '$group' from '",
                     $self->str, q{': }, $_);
      };
    }
    else {
      $num_errors++;
      $self->error("Attempted to remove permissions for non-existent group ",
                   "'$group' on '", $self->str, q{'});
    }
  }

  $self->debug("Groups to add: [", join(', ', @to_add), "]");

  foreach my $group (@to_add) {
    if (not $strict_groups or any { $group eq $_ } @all_groups) {
      try {
        $self->set_permissions('read', $group);
      } catch {
        $num_errors++;
        $self->error("Failed to add read permissions for group '$group' to '",
                     $self->str, q{': }, $_);
      };
    }
    else {
      $num_errors++;
      $self->error("Attempted to add read permissions for non-existent group ",
                   "'$group' on '", $self->str, q{'});
    }
  }

  if ($num_errors > 0) {
    my $msg = "Failed to update cleanly group permissions on '" . $self->str .
      "'; $num_errors errors were recorded. See logs for details ".
      "(strict groups = $strict_groups).";

    if ($strict_groups) {
      $self->logconfess($msg);
    }
    else {
      $self->error($msg);
    }
  }

  return $self;
}

=head2 str

  Arg [1]    : None

  Example    : $path->str
  Description: Return an absolute path string in iRODS.
  Returntype : Str

=cut

sub str {
  my ($self) = @_;

  return File::Spec->join($self->collection, $self->data_object);
}

=head2 json

  Arg [1]    : None

  Example    : $path->json
  Description: Return a canonical JSON representation of this path,
               including any AVUs.
  Returntype : Str

=cut

sub json {
  my ($self) = @_;

  my $spec = {collection  => $self->collection,
              data_object => $self->data_object,
              avus        => $self->metadata};

  return $self->encode($spec);
}

sub slurp {
  my ($self) = @_;

  my $content = $self->irods->slurp_object($self->str);

  defined $content or
    $self->logconfess("Slurped content of '", $self->str, "' was undefined");

  return $content;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::DataObject - An iRODS data object.

=head1 DESCRIPTION

Represents a data object and provides methods for adding and removing
metdata, applying checksums and setting access permissions.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2013, 2014, 2015 Genome Research Limited. All Rights
Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
