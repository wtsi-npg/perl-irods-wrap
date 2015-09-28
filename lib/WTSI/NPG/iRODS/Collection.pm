
package WTSI::NPG::iRODS::Collection;

use File::Spec;
use Moose;

use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;

our $VERSION = '';

with 'WTSI::NPG::iRODS::Path';

# Lazily load metadata from iRODS
around 'metadata' => sub {
  my ($orig, $self) = @_;

  unless ($self->has_metadata) {
    my @meta = $self->irods->get_collection_meta($self->str);
    $self->$orig(\@meta);
  }

  return $self->$orig;
};

=head2 is_present

  Arg [1]    : None

  Example    : $path->is_present && print $path->str
  Description: Return true if the collection exists in iRODS.
  Returntype : WTSI::NPG::iRODS::Collection

=cut

sub is_present {
  my ($self) = @_;

  return $self->irods->list_collection($self->str);
}

=head2 absolute

  Arg [1]    : None

  Example    : $path->absolute
  Description: Return the absolute path of the collection.
  Returntype : WTSI::NPG::iRODS::Collection

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

    $absolute = File::Spec->catdir($self->irods->working_collection,
                                   $self->collection);
  }

  return WTSI::NPG::iRODS::Collection->new($self->irods, $absolute);
}

=head2 add_avu

  Arg [1]    : attribute
  Arg [2]    : value
  Arg [2]    : units (optional)

  Example    : $path->add_avu('foo', 'bar')
  Description: Add an AVU to an iRODS path (data object or collection)
               Return self. Clear the metadata cache.
  Returntype : WTSI::NPG::iRODS::Collection

=cut

sub add_avu {
  my ($self, $attribute, $value, $units) = @_;

  if ($self->find_in_metadata($attribute, $value, $units)) {
    my $units_str = defined $units ? "'$units'" : 'undef';
    $self->debug("Failed to add AVU {'$attribute', '$value', $units_str} ",
                 "to '", $self->str, "': AVU is already present");
  }
  else {
    $self->irods->add_collection_avu($self->str, $attribute, $value, $units);
  }

  $self->clear_metadata;

  return $self;
}

=head2 remove_avu

  Arg [1]    : attribute
  Arg [2]    : value
  Arg [2]    : units (optional)

  Example    : $path->remove_avu('foo', 'bar')
  Description: Remove an AVU from an iRODS path (data object or collection)
               Return self. Clear the metadata cache.
  Returntype : WTSI::NPG::iRODS::Collection

=cut

sub remove_avu {
  my ($self, $attribute, $value, $units) = @_;

  if ($self->find_in_metadata($attribute, $value, $units)) {
    $self->irods->remove_collection_avu($self->str, $attribute, $value, $units);
  }
  else {
    my $units_str = defined $units ? "'$units'" : 'undef';
    $self->logcarp("Failed to remove AVU {'$attribute', '$value', $units_str} ",
                   "from '", $self->str, "': AVU is not present");
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

  return $self->irods->make_collection_avu_history
    ($self->str, $attribute, $timestamp);
}

=head2 get_contents

  Arg [1]    :

  Example    : my ($objs, $cols) = $coll->get_contents
  Description: Return the contents of the collection as two arrayrefs,
               the first listing data objects, the second listing nested
               collections.
  Returntype : Array

=cut

sub get_contents {
   my ($self, $recurse, $checksums) = @_;

   my $irods = $self->irods;
   my $path = $self->str;
   my ($objs, $colls) = $self->irods->list_collection($path, $recurse);

   my $object_checksums;
   if ($checksums) {
     $object_checksums = $self->irods->collection_checksums($path, $recurse);
   }

   my @objects;
   my @collections;

   foreach my $obj (@$objs) {
     my $object = WTSI::NPG::iRODS::DataObject->new($irods, $obj);

     if ($checksums) {
       if (exists $object_checksums->{$obj}) {
         $object->checksum($checksums->{$obj});
       }
       else {
         $self->logwarn("Failed to find a checksum for '$obj' when getting ",
                        "the contents of '$path'");
       }
     }

     push @objects, $object;
   }
   foreach my $coll (@$colls) {
     push @collections, WTSI::NPG::iRODS::Collection->new($irods, $coll);
   }

   return (\@objects, \@collections);
}

=head2 get_permissions

  Arg [1]    : None

  Example    : $coll->get_permissions
  Description: Return a list of ACLs defined for the collection.
  Returntype : Array

=cut

sub get_permissions {
  my ($self) = @_;

  my $path = $self->str;
  return $self->irods->get_collection_permissions($path);
}

=head2 set_permissions

  Arg [1]    : Permission, Str. One of $WTSI::NPG::iRODS::READ_PERMISSION,
               $WTSI::NPG::iRODS::WRITE_PERMISSION,
               $WTSI::NPG::iRODS::OWN_PERMISSION or
               $WTSI::NPG::iRODS::NULL_PERMISSION.
  Arg [2]    : Array of owners (users and /or groups).

  Example    : $coll->set_permissions('read', 'user1', 'group1')
  Description: Set access permissions on the collection. Return self.
  Returntype : WTSI::NPG::iRODS::Collection

=cut

sub set_permissions {
  my ($self, $permission, @owners) = @_;

  my $perm_str = defined $permission ? $permission :
    $WTSI::NPG::iRODS::NULL_PERMISSION;

  my $path = $self->str;
  foreach my $owner (@owners) {
    $self->info("Giving owner '$owner' '$perm_str' access to '$path'");
    $self->irods->set_collection_permissions($perm_str, $owner, $path);
  }

  return $self;
}

=head2 get_groups

  Arg [1]    : Permission, Str. One of $WTSI::NPG::iRODS::READ_PERMISSION,
               $WTSI::NPG::iRODS::WRITE_PERMISSION,
               $WTSI::NPG::iRODS::OWN_PERMISSION or
               $WTSI::NPG::iRODS::NULL_PERMISSION. Optional.

  Example    : $coll->get_groups($WTSI::NPG::iRODS::READ_PERMISSION)
  Description: Return a list of the data access groups in the collection's ACL.
               If a permission level argument is supplied, only groups with
               that level of access will be returned. Only groups having a
               group name matching the current group filter will be returned.
  Returntype : Array

=cut

sub get_groups {
  my ($self, $level) = @_;

  return $self->irods->get_collection_groups($self->str, $level);
}

=head2 set_content_permissions

  Arg [1]    : Permission, Str. One of $WTSI::NPG::iRODS::READ_PERMISSION,
               $WTSI::NPG::iRODS::WRITE_PERMISSION,
               $WTSI::NPG::iRODS::OWN_PERMISSION or
               $WTSI::NPG::iRODS::NULL_PERMISSION.
  Arg [2]    : Array of owners (users and /or groups).

  Example    : $coll->set_content_permissions
                 ($WTSI::NPG::iRODS::READ_PERMISSION, 'user1', 'group1')
  Description: Recursively set access permissions on the collection and
               its contents. Return self.
  Returntype : WTSI::NPG::iRODS::Collection

=cut

sub set_content_permissions {
  my ($self, $permission, @owners) = @_;

  my $perm_str = defined $permission ? $permission :
    $WTSI::NPG::iRODS::NULL_PERMISSION;

  my $path = $self->str;
  my ($objects, $collections) = $self->irods->list_collection($path, 'RECURSE');

  foreach my $owner (@owners) {
    foreach my $object (@$objects) {
      $self->info("Giving owner '$owner' '$permission' access to '$object'");
      $self->irods->set_object_permissions($perm_str, $owner, $object);
    }

    foreach my $collection (@$collections) {
      $self->info("Giving owner '$owner' '$permission' access ",
                  "to '$collection'");
      $self->irods->set_collection_permissions($perm_str, $owner, $collection);
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

  return $self->collection;
}

=head2 json

  Arg [1]    : None

  Example    : $path->str
  Description: Return a canonical JSON representation of this path,
               including any AVUs.
  Returntype : Str

=cut

sub json {
  my ($self) = @_;

  my $spec = {collection => $self->collection,
              avus       => $self->metadata};

  return $self->encode($spec);
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::Collection - An iRODS collection.

=head1 DESCRIPTION

Represents a collection and provides methods for adding and removing
metdata and setting access permissions.

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
