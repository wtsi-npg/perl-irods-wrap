
package WTSI::NPG::iRODS::Path;

use File::Spec;
use List::AllUtils qw(uniq);
use Moose::Role;

use WTSI::NPG::iRODS;

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::NPG::Annotatable',
  'WTSI::DNAP::Utilities::JSONCodec';

has 'collection' =>
  (is        => 'ro',
   isa       => 'Str',
   required  => 1,
   lazy      => 1,
   default   => q{.},
   predicate => 'has_collection');

has 'irods' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS',
   required => 1);

has 'metadata' => (is        => 'rw',
                   isa       => 'ArrayRef',
                   predicate => 'has_metadata',
                   clearer   => 'clear_metadata');

requires
  'absolute',
  'add_avu',
  'get_groups',
  'get_permissions',
  'is_present',
  'make_avu_history',
  'remove_avu',
  'set_permissions';

around BUILDARGS => sub {
  my ($orig, $class, @args) = @_;

  if (@args == 2 && ref $args[0] eq 'WTSI::NPG::iRODS') {
    return $class->$orig(irods      => $args[0],
                         collection => $args[1]);
  }
  else {
    return $class->$orig(@_);
  }
};

sub BUILD {
  my ($self) = @_;

  # Make our logger be the iRODS logger by default
  $self->logger($self->irods->logger);

  return $self;
}

around 'metadata' => sub {
   my ($orig, $self, @args) = @_;

   my @sorted = sort {
     $a->{attribute} cmp $b->{attribute}                    ||
     $a->{value}     cmp $b->{value}                        ||
     (( defined $a->{units} && !defined $b->{units}) && -1) ||
     ((!defined $a->{units} &&  defined $b->{units}) &&  1) ||
     $a->{units}     cmp $b->{units} } @{$self->$orig(@args)};

   return \@sorted;
};

=head2 get_avu

  Arg [1]    : attribute
  Arg [2]    : value (optional)
  Arg [2]    : units (optional)

  Example    : $path->get_avu('foo')
  Description: Return a single matching AVU. If multiple candidate AVUs
               match the arguments, an error is raised.
  Returntype : HashRef

=cut

sub get_avu {
  my ($self, $attribute, $value, $units) = @_;
  $attribute or $self->logcroak("An attribute argument is required");

  my @exists = $self->find_in_metadata($attribute, $value, $units);

  # If the AVU does not exist, return an empty HashRef. This way the
  # caller does not need to check for undef before dereferencing. The
  # caller can simply check $obj->get_avu('foo')->{attribute}.
  # my $avu = {};

  my $avu;

  if (@exists) {
    if (scalar @exists == 1) {
      $avu = $exists[0];
    }
    else {
      $value ||= q{};
      $units ||= q{};

      my $fn = sub {
        my $elt = shift;

        my $a = defined $avu->{attribute} ? $elt->{attribute} : 'undef';
        my $v = defined $avu->{value}     ? $elt->{value}     : 'undef';
        my $u = defined $avu->{units}     ? $elt->{units}     : 'undef';

        return sprintf "{'%s', '%s', '%s'}", $a, $v, $u;
      };

      my $matched = join ", ", map { $fn->($_) } @exists;

      $self->logconfess("Failed to get a single AVU matching ",
                        "{'$attribute', '$value', '$units'}: ",
                        "matched [$matched]");
    }
  }

  return $avu;
}

=head2 find_in_metadata

  Arg [1]    : attribute
  Arg [2]    : value (optional)
  Arg [2]    : units (optional)

  Example    : my @avus = $path->find_in_metadata('foo')
  Description: Return all matching AVUs
  Returntype : Array

=cut

sub find_in_metadata {
  my ($self, $attribute, $value, $units) = @_;
  $attribute or $self->logcroak("An attribute argument is required");

  my @meta = @{$self->metadata};
  my @exists;

  if (defined $value && defined $units) {
    @exists = grep { $_->{attribute} eq $attribute &&
                     $_->{value}     eq $value &&
                     $_->{units}     eq $units } @meta;
  }
  elsif (defined $value) {
    @exists = grep { $_->{attribute} eq $attribute &&
                     $_->{value}     eq $value } @meta;
  }
  else {
    @exists = grep { $_->{attribute} eq $attribute } @meta;
  }

  return @exists;
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
    $history_avu = $self->make_avu_history($attribute, $timestamp);
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

=head2 expected_groups

  Arg [1]    : None

  Example    : @groups = $path->expected_groups
  Description: Return an array of iRODS group names given metadata containing
               >=1 study_id under the key Annotation::study_id_attr
  Returntype : Array

=cut

sub expected_groups {
  my ($self) = @_;

  my @ss_study_avus = $self->find_in_metadata($self->study_id_attr);

  my @groups;
  foreach my $avu (@ss_study_avus) {
    my $study_id = $avu->{value};
    my $group = $self->irods->make_group_name($study_id);
    push @groups, $group;
  }

  return @groups;
}

sub meta_str {
  my ($self) = @_;

  return $self->json;
}

=head2 meta_json

  Arg [1]    : None

  Example    : $json = $path->meta_json
  Description: Return all metadata as UTF-8 encoded JSON.
  Returntype : Str

=cut

sub meta_json {
  my ($self) = @_;

  return $self->encode($self->metadata);
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

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::Path - The base class for representing iRODS
collections and data objects.

=head1 DESCRIPTION

Represents the features common to all iRODS paths; the collection and
the metadata.

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
