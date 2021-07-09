package WTSI::NPG::iRODS::DataObject;

use namespace::autoclean;
use File::Spec;
use List::MoreUtils qw(none uniq);
use Moose;
use MooseX::StrictConstructor;
use Set::Scalar;
use Try::Tiny;

use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::Metadata qw($SAMPLE_CONSENT
                                  $SAMPLE_CONSENT_WITHDRAWN);
use WTSI::NPG::iRODS::Replicate;
use WTSI::NPG::iRODS::Types qw(ArrayRefOfReplicate);

our $VERSION = '';

our $EMPTY_FILE_CHECKSUM = q[d41d8cd98f00b204e9800998ecf8427e];

with 'WTSI::NPG::iRODS::Path';

has 'data_object' =>
  (is            => 'ro',
   isa           => 'Str',
   required      => 1,
   lazy          => 1,
   default       => q{.},
   predicate     => 'has_data_object',
   documentation => 'The data object component of the iRODS path.');

has 'checksum' =>
  (is            => 'ro',
   isa           => 'Str',
   lazy          => 1,
   builder       => '_build_checksum',
   predicate     => 'has_checksum',
   clearer       => 'clear_checksum',
   documentation => 'The checksum of the data object.');

has 'size' =>
  (is            => 'ro',
   isa           => 'Int',
   lazy          => 1,
   builder       => '_build_size',
   predicate     => 'has_size',
   clearer       => 'clear_size',
   documentation => 'The size in bytes of the data object in the catalog. ' .
                    'This is the value that iRODS reports for the whole ' .
                    'data object. Each replicate, if any, also has its own ' .
                    'size value.');

# TODO: Add a check so that a DataObject cannot be built from a path
# that is in fact a collection.
around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  if (@args >= 2 && ref $args[0]) {
    my ($irods, $path, @rest) = @args;
    my ($volume, $collection, $data_name) = File::Spec->splitpath($path);
    $collection = File::Spec->canonpath($collection);
    $collection ||= q{.};

    return $class->$orig(irods       => $irods,
                         collection  => $collection,
                         data_object => $data_name,
                         @rest);
  }
  else {
    return $class->$orig(@args);
  }
};

# Lazily load checksum from iRODS
sub _build_checksum {
  my ($self) = @_;

  return $self->irods->checksum($self->str);
}

# Lazily load size from iRODS
sub _build_size {
  my ($self) = @_;

  return $self->irods->size($self->str);
}

=head2 replicates

  Arg [1]    : None.

  Example    : my @replicates = $obj->replicates
  Description: Return an array of all replicates for a data
               object, sorted by ascending replicate number.
  Returntype : Array[WTSI::NPG::iRODS::Replicate]

=cut

sub replicates {
  my ($self) = @_;

  my @replicates = sort { $a->number cmp $b->number }
                    map { WTSI::NPG::iRODS::Replicate->new($_) }
                    $self->irods->replicates($self->str);
  return @replicates;
}

=head2 valid_replicates

  Arg [1]    : None.

  Example    : my @replicates = $obj->valid_replicates
  Description: Return an array of all valid replicates for a data
               object, sorted by ascending replicate number.
  Returntype : Array[WTSI::NPG::iRODS::Replicate]

=cut

sub valid_replicates {
  my ($self) = @_;

  my @valid_replicates = sort { $a->number cmp $b->number }
    grep { $_->is_valid } $self->replicates;

  return @valid_replicates;
}

=head2 invalid_replicates

  Arg [1]    : None.

  Example    : my @replicates = $obj->invalid_replicates
  Description: Return an array of all invalid replicates for a data
               object, sorted by ascending replicate number.
  Returntype : Array[WTSI::NPG::iRODS::Replicate]

=cut

sub invalid_replicates {
  my ($self) = @_;

  my @invalid_replicates = sort { $a->number cmp $b->number }
    grep { not $_->is_valid } $self->replicates;

  return @invalid_replicates;
}

=head2 prune_replicates

  Arg [1]    : None.

  Example    : my @pruned = $obj->prune_replicates
  Description: Remove any replicates of a data object that are marked as
               stale in the ICAT.  Return an array of descriptors of the
               pruned replicates.  Raise an error if there are only
               invalid replicates; there should always be a valid replicate
               and pruning in this case would be equivalent to deletion.
  Returntype : Array[WTSI::NPG::iRODS::Replicate]

=cut

sub prune_replicates {
  my ($self) = @_;

  my @invalid_replicates = $self->invalid_replicates;
  my $path = $self->str;

  my @pruned;
  if ($self->valid_replicates) {

    foreach my $rep (@invalid_replicates) {
      my $resource = $rep->resource;
      my $checksum = $rep->checksum;
      my $number   = $rep->number;
      $self->debug("Pruning invalid replicate $number with checksum ",
                   "'$checksum' from resource '$resource' for ",
                   "data object '$path'");
      $self->irods->remove_replicate($path, $number);
      push @pruned, $rep;
    }

    $self->calculate_checksum;
  }
  else {
    $self->logconfess("Failed to prune invalid replicates from '$path': ",
                      "there and no valid replicates of this data object; ",
                      "pruning would be equivalent to deletion");
  }

  return @pruned;
}

=head2 remove_replicate

  Arg [1]    : Replicate number, Int.

  Example    : $obj->remove_replicate($replicate_num)
  Description: Remove a replicate of a data object.  Return $self.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub remove_replicate {
  my ($self, $replicate_num) = @_;

  $self->irods->remove_replicate($self->str, $replicate_num);
  $self->calculate_checksum;

  return $self;
}

sub get_metadata {
  my ($self) = @_;

  return [$self->irods->get_object_meta($self->str)];
}

=head2 is_present

  Arg [1]    : None

  Example    : $path->is_present && print $path->str
  Description: Return true if the data object file exists in iRODS.
  Returntype : Bool

=cut

sub is_present {
  my ($self) = @_;

  return $self->irods->list_object($self->str);
}

=head2 is_consistent_size

  Arg [1]    : None

  Example    : $path->is_consistent_size && print $path->str
  Description: Return true if the data object in iRODS is internally
               consistent. This is defined as:

               1. If the file is zero length, it has the checksum of an
                  empty file.
               2. If the file is not zero length, it does not have the checksum
                  of an empty file.

               This method looks for data object size and checksum consistency.
               It checks the values that iRODS reports for the whole data
               object; it does not check individual replicates.

               If the data object is absent, this method returns true as there
               can be no conflict where neither value exists.

               If the data object has no checksum, this method returns true as
               there is no evidence to dispute its reported size.

               In iRODS <= 4.2.8 it is possible for a data object to get into a
               bad state where it has zero length, but still reports as not
               stale and having the checksum of the full-length file.

               We can trigger this behaviour in iRODS by having more than one
               client uploading to a single path. iRODS does not support any
               form of locking, allows uncoordinated writes to the
               filesystem. It does recognise this as a failure, but does not
               clean up the damaged file.

  Returntype : Bool

=cut

sub is_consistent_size {
  my ($self) = @_;

  if (not $self->is_present) {
    return 1;
  }

  if (not $self->checksum) {
    # This return is redundant as the checksum method call will trigger an
    # exception if no checksum is present in iRODS (due to the isa
    # constraint on the checksum attribute).
    return 1;
  }

  if ($self->size == 0) {
    return $self->checksum eq $EMPTY_FILE_CHECKSUM;
  }
  else {
    return $self->checksum ne $EMPTY_FILE_CHECKSUM;
  }
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

  Arg [1]    : Permission, Str. One of $WTSI::NPG::iRODS::READ_PERMISSION,
               $WTSI::NPG::iRODS::WRITE_PERMISSION,
               $WTSI::NPG::iRODS::OWN_PERMISSION or
               $WTSI::NPG::iRODS::NULL_PERMISSION.
  Arg [2]    : Array of owners (users and /or groups). These may be of the
               form <user> or <user>#<zone>.

  Example    : $obj->set_permissions($WTSI::NPG::iRODS::READ_PERMISSION,
                                     'user1', 'group1')
  Description: Set access permissions on the object. Return self.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub set_permissions {
  my ($self, $permission, @owners) = @_;

  my $perm_str = defined $permission ? $permission :
    $WTSI::NPG::iRODS::NULL_PERMISSION;

  my $path = $self->str;
  foreach my $owner (@owners) {
    $self->info("Giving owner '$owner' '$perm_str' access to '$path'");
    $self->irods->set_object_permissions($perm_str, $owner, $path);
  }

  return $self;
}

=head2 get_groups

  Arg [1]      Permission, Str.  One of $WTSI::NPG::iRODS::READ_PERMISSION,
               $WTSI::NPG::iRODS::WRITE_PERMISSION,
               $WTSI::NPG::iRODS::OWN_PERMISSION or
               $WTSI::NPG::iRODS::NULL_PERMISSION. Optional.

  Example    : $obj->get_object_groups($WTSI::NPG::iRODS::READ_PERMISSION)
  Description: Return a list of the data access groups in the object's ACL.
               If a permission level argument is supplied, only groups with
               that level of access will be returned. Only groups having a
               group name matching the current group filter will be returned.
  Returntype : Array

=cut

sub get_groups {
  my ($self, $level) = @_;

  return $self->irods->get_object_groups($self->str, $level);
}

=head2 update_group_permissions

  Arg [1]      Strictly check groups, Bool. Optional, defaults to false.
               If true, no attempt will be made to operate on an apparently
               non-existent groups. Due to a bug in 'igroupadmin lg`, groups
               in other zones may be invisible. With strict group checking
               off, operations on invsisble groups will be attempted. Strict
               group checking makes any errors fatal, otherwise errors are
               logged only.

  Example    : $obj->update_group_permissions
  Description: Modify a data objects ACL with respect to its study_id and
               sample_consent / consent_withdrawn metadata and return the
               data object.

               The target group membership is determined by the result of
               calling $self->expected_groups. The current group membership
               is determined and any difference calculated. Unwanted
               group memberships are pruned, then missing group memberships
               are added.

               If there are sample_consent or consent_withdrawn metadata,
               access for all groups is removed.

               This method does not add or remove access for the 'public'
               group.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub update_group_permissions {
  my ($self, $strict_groups) = @_;

  $strict_groups = $strict_groups ? 1 : 0;
  # If strict_groups is true, we only work with groups we can see with
  # igroupadmin. Across zones we usually have to work non-strict
  # because the stock igroupadmin can't see them.

  # Record the current group permissions
  my @groups_permissions =
    $self->get_groups($WTSI::NPG::iRODS::READ_PERMISSION);
  my @groups_annotated = $self->expected_groups;

  $self->debug('Permissions before: [', join(', ', @groups_permissions), ']');
  $self->debug('Updated annotations: [', join(', ', @groups_annotated), ']');

  my $path = $self->str;

  my $true  = 1;
  my $false = 0;
  if ($self->get_avu($SAMPLE_CONSENT,          $false) or
      $self->get_avu($SAMPLE_CONSENT_WITHDRAWN, $true)) {
    $self->info('Data is marked as CONSENT WITHDRAWN; ',
                'all permissions will be withdrawn');
    @groups_annotated = (); # Emptying this means all will be removed
  }

  my $perms = Set::Scalar->new(@groups_permissions);
  my $annot = Set::Scalar->new(@groups_annotated);
  my @to_remove = $perms->difference($annot)->members;
  my @to_add    = $annot->difference($perms)->members;

  $self->debug('Groups to remove: [', join(', ', @to_remove), ']');

  # We try/catch for each group in order to do our best, while
  # counting any errors and failing afterwards if the update was not
  # clean.
  my $num_errors = 0;

  my @all_groups = $self->irods->groups; # Use group cache
  foreach my $group (@to_remove) {
    if ($strict_groups and none { $group eq $_ } @all_groups) {
      $num_errors++;
      $self->error('Attempted to remove permissions for non-existent group ',
                   "'$group' on '$path'");
    }
    else {
      try {
        $self->set_permissions($WTSI::NPG::iRODS::NULL_PERMISSION, $group);
      } catch {
        $num_errors++;
        my @stack = split /\n/msx; # Chop up the stack trace
        $self->error("Failed to remove permissions for group '$group' from ",
                     "'$path': ", pop @stack);
      };
    }
  }

  $self->debug('Groups to add: [', join(', ', @to_add), ']');

  foreach my $group (@to_add) {
    if ($strict_groups and none { $group eq $_ } @all_groups) {
      $num_errors++;
      $self->error("Attempted to add read permissions for non-existent group ",
                   "'$group' to '$path'");
    }
    else {
      try {
        $self->set_permissions($WTSI::NPG::iRODS::READ_PERMISSION, $group);
      } catch {
        $num_errors++;
        my @stack = split /\n/msx; # Chop up the stack trace
        $self->error("Failed to add read permissions for group '$group' to ",
                     "'$path': ", pop @stack);
      };
    }
  }

  if ($num_errors > 0) {
    my $msg = "Failed to update cleanly group permissions on '$path': " .
      "$num_errors errors were recorded. See logs for details ".
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

See also WTSI::NPG::iRODS::Path.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2013, 2014, 2015, 2016, 2021 Genome Research Limited. All
Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
