
package WTSI::NPG::iRODS::DataObject;

use File::Spec;
use List::AllUtils qw(uniq);
use Moose;
use Set::Scalar;

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

  Example    : $path->calculate_checksum
  Description: Return the MD5 checksum of the data object.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub calculate_checksum {
  my ($self) = @_;

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

=head2 supersede_avus

  Arg [1]    : Str attribute
  Arg [2]    : Str value
  Arg [3]    : Str units (optional)
  Arg [4]    : DateTime timestamp (optional) to use in creation of the
               AVU history

  Example    : $path->supersede_avus('foo', 'bar')
  Description: Replace any existing AVUs on an iRODS data object
               with a single new AVU having the same attribute. If there
               are no existing AVUs having the specified attribute, simply
               add the new AVU. Return self. Clear the metadata cache.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub supersede_avus {
  my ($self, $attribute, $value, $units, $timestamp) = @_;

  ref $value and
    $self->logcroak("The value argument must be a scalar");

  return $self->supersede_multivalue_avus($attribute, [$value], $units,
                                          $timestamp);
}

=head2 supersede_multivalue_avus

  Arg [1]    : Str attribute
  Arg [2]    : ArrayRef[Str] values
  Arg [3]    : Str units (optional)
  Arg [4]    : DateTime timestamp (optional) to use in creation of the
               AVU history

  Example    : $path->supersede_multivalue_avus('foo', ['bar', 'baz'])
  Description: Replace any existing AVUs on an iRODS data object
               with a set of new AVUs having the same attribute and the
               specified values. If there are no existing AVUs having
               the specified attribute, simply add the new AVUs.
               Return self. Clear the metadata cache.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub supersede_multivalue_avus {
  my ($self, $attribute, $values, $units, $timestamp) = @_;

  defined $attribute or
    $self->logcroak("A defined attribute argument is required");
  defined $values or
    $self->logcroak("A defined values argument is required");

  ref $values eq 'ARRAY' or
    $self->logcroak("The values argument must be an ArrayRef");

  $self->debug("Superseding all '$attribute' AVUs on '", $self->str, q{'});

  my $history_avu;
  if (!$self->irods->is_avu_history_attr($attribute)) {
    $history_avu = $self->irods->make_object_avu_history
      ($self->str, $attribute, $timestamp);
  }

  my @values = uniq @$values;

  my @old_avus = $self->find_in_metadata($attribute);
  my @new_avus = map { {attribute => $attribute,
                        value     => $_,
                        units     => $units} } @values;

  # Compare old AVUS to new; if any of the new ones are already
  # present, leave the old copy, otherwise remove the old AVU
  my $num_old = scalar @old_avus;
  $self->debug("Found $num_old existing '$attribute' AVUs on '",
               $self->str, q{'});

  my $num_old_processed = 0;
  my $num_old_removed   = 0;
  my @retained_avus;
 OLD: foreach my $old_avu (@old_avus) {
    $num_old_processed++;

    foreach my $new_avu (@new_avus) {
      if (_avus_equal($old_avu, $new_avu)) {
        $self->debug("Not superseding (retaining) old AVU ",
                     _avu_str($old_avu), " on '", $self->str,
                     "' [$num_old_processed / $num_old]");
        push @retained_avus, $old_avu;
        next OLD;
      }
      else {
        $self->debug("Superseding (removing) old AVU ",
                     _avu_str($old_avu), " on '", $self->str,
                     "' [$num_old_processed / $num_old]");
        $self->remove_avu($old_avu->{attribute},
                          $old_avu->{value},
                          $old_avu->{units});
        $num_old_removed++;
        next OLD;
      }
    }
  }

  # Add the new AVUs, unless they are identical to one of the old
  # copies that were retained
  my $num_new = scalar @new_avus;
  $self->debug("Adding $num_new '$attribute' AVUs to '", $self->str, q{'});

  my $num_new_processed = 0;
  my $num_new_added     = 0;
 NEW: foreach my $new_avu (@new_avus) {
    $num_new_processed++;

    foreach my $old_avu (@retained_avus) {
      if (_avus_equal($new_avu, $old_avu)) {
        $self->debug("Superseding (using retained) new AVU ",
                     _avu_str($old_avu), " on '", $self->str,
                     "' [$num_new_processed / $num_new]");
        next NEW;
      }
    }

    # If we can't re-use a retained AVU, we must add this one
    $self->debug("Superseding (adding) new AVU ",
                 _avu_str($new_avu), " on '", $self->str,
                 "' [$num_new_processed / $num_new]");
    $self->add_avu($new_avu->{attribute},
                   $new_avu->{value},
                   $new_avu->{units});
    $num_new_added++;
  }

  # Only add history if some AVUs were removed or added
  if (($num_old_removed > 0 || $num_new_added > 0) && defined $history_avu) {
      my $history_attribute = $history_avu->{attribute};
      my $history_value     = $history_avu->{value};
      $self->debug("Adding history AVU ",
                   "{'$history_attribute', '$history_value', ''} to ",
                   $self->str);
      $self->add_avu($history_attribute, $history_value);
    }

  return $self;
}

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

sub get_groups {
  my ($self, $level) = @_;

  return $self->irods->get_object_groups($self->str, $level);
}

sub update_group_permissions {
  my ($self) = @_;

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
  $self->set_permissions('null', @to_remove);
  $self->debug("Groups to add: [", join(', ', @to_add), "]");
  $self->set_permissions('read', @to_add);

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

sub _avus_equal {
  my ($new_avu, $old_avu) = @_;

  return ((defined $new_avu->{units} && defined $old_avu->{units} &&
           $new_avu->{attribute} eq $old_avu->{attribute} &&
           $new_avu->{value}     eq $old_avu->{value} &&
           $new_avu->{units}     eq $old_avu->{units})
          ||
          (!defined $new_avu->{units} && !defined $old_avu->{units} &&
           $new_avu->{attribute} eq $old_avu->{attribute} &&
           $new_avu->{value}     eq $old_avu->{value}));
}

sub _avu_str {
  my ($avu) = @_;

  my ($attribute, $value, $units) =
    map { defined $_ ? $_ : 'undef' }
      ($avu->{attribute}, $avu->{value}, $avu->{units});

  return sprintf "{'%s', '%s', '%s'}", $attribute, $value, $units;
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

Copyright (c) 2013-2014 Genome Research Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
