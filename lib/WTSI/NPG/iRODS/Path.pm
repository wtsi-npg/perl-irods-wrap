package WTSI::NPG::iRODS::Path;

use Data::Dump qw(pp);
use File::Spec;
use List::MoreUtils qw(any notall uniq);
use Moose::Role;

use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::Metadata qw($STUDY_ID
                                  $ALIGNMENT_FILTER
                                  $SAMPLE_CONSENT
                                  $SAMPLE_CONSENT_WITHDRAWN);

our $VERSION = '';

with 'WTSI::DNAP::Utilities::Loggable', 'WTSI::DNAP::Utilities::JSONCodec',
  'WTSI::NPG::iRODS::Utilities';

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

has 'metadata' => (is            => 'ro',
                   isa           => 'ArrayRef',
                   lazy          => 1,
                   builder       => 'get_metadata',
                   predicate     => 'has_metadata',
                   clearer       => 'clear_metadata',
                   documentation => 'iRODS metadata as AVU HashRefs');

requires
  'absolute',
  'add_avu',
  'get_metadata',
  'get_groups',
  'get_permissions',
  'is_present',
  'make_avu_history',
  'remove_avu',
  'set_permissions';

# TODO: In next major version, make method names more consistent and
# document the conventions used. e.g. checksum accessor is 'checksum',
# not 'get_checksum'.
#
# Maybe the 'get_' prefix could be removed? It's there at the moment
# because I wanted 'get' and 'set' operations to be clearly different
# methods. Maybe the 'get_' prefix should be added to those methods
# (like 'checksum') that do not have it? Maybe groups and permissions
# should be attributes?
#
# There should be a convention for methods that fetch fresh data from
# iRODS and those that may cache the values from iRODS (until
# cleared).

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

  defined $attribute or
    $self->logcroak("An attribute argument is required");
  $attribute eq q{} and
    $self->logcroak('A non-empty attribute argument is required');

  my @exists = $self->find_in_metadata($attribute, $value, $units);

  # If the AVU does not exist, return an empty HashRef. This way the
  # caller does not need to check for undef before dereferencing. The
  # caller can simply check $obj->get_avu('foo')->{attribute}.
  # my $avu = {};
  # The above is commented because I'm not sure that I like the
  # consequences as an API user.

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

        my $a = defined $elt->{attribute} ? $elt->{attribute} : 'undef';
        my $v = defined $elt->{value}     ? $elt->{value}     : 'undef';
        my $u = defined $elt->{units}     ? $elt->{units}     : 'undef';

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

  defined $attribute or
    $self->logcroak("An attribute argument is required");
  $attribute eq q{} and
    $self->logcroak('A non-empty attribute argument is required');

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

=head2 abandon_avus

  Arg [1]    : Str attribute
  Arg [2]    : DateTime timestamp (optional) to use in creation of the
               AVU history

  Example    : $path->abandon_avus('foo')
  Description: Remove any existing AVUs on an iRODS path. Think of this
               as superseding AVUs with an empty set. Return self.
               Clear the metadata cache.
  Returntype : WTSI::NPG::iRODS::Path

=cut

sub abandon_avus {
  my ($self, $attribute, $timestamp) = @_;

  defined $attribute or
    $self->logcroak("An attribute argument is required");
  $attribute eq q{} and
    $self->logcroak('A non-empty attribute argument is required');

  $self->debug("Abandoning all '$attribute' AVUs on '", $self->str, q{'});

  return $self->_update_multivalue_avus($attribute, [], undef, $timestamp);
}

=head2 supersede_avus

  Arg [1]    : Str attribute
  Arg [2]    : Str value
  Arg [3]    : Str units (optional)
  Arg [4]    : DateTime timestamp (optional) to use in creation of the
               AVU history

  Example    : $path->supersede_avus('foo', 'bar')
  Description: Replace any existing AVUs on an iRODS path
               with a single new AVU having the same attribute. If there
               are no existing AVUs having the specified attribute, simply
               add the new AVU. Return self. Clear the metadata cache.
  Returntype : WTSI::NPG::iRODS::Path

=cut

sub supersede_avus {
  my ($self, $attribute, $value, $units, $timestamp) = @_;

  ref $value and $self->logcroak("The value argument must be a scalar");

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
  Description: Replace any existing AVUs on an iRODS path
               with a set of new AVUs having the same attribute and the
               specified values. If there are no existing AVUs having
               the specified attribute, simply add the new AVUs.
               Return self. Clear the metadata cache.
  Returntype : WTSI::NPG::iRODS::Path

=cut

sub supersede_multivalue_avus {
  my ($self, $attribute, $values, $units, $timestamp) = @_;

  defined $attribute or
    $self->logcroak("An attribute argument is required");
  defined $values or
    $self->logcroak("A defined values argument is required");

  ref $values eq 'ARRAY' or
    $self->logcroak("The values argument must be an ArrayRef");

  my $path = $self->str;
  my @values = @$values;
  if (notall { defined } @values) {
    $self->logcarp("The values array for '$attribute' contained one or more ",
                   "undef elements. An AVU may not have an undef value; any ",
                   "such will be ignored.");
    @values = grep { defined } @values;
  }

  @values = uniq @values;
  @values or $self->logcroak("At least one defined value is required");

  $self->debug("Superseding all '$attribute' AVUs on '$path'");

  my ($added, $removed, $retained) =
    $self->_update_multivalue_avus($attribute, \@values, $units, $timestamp);

  if (@{$removed}) {
    $self->info("Removed metadata AVUs from '$path': ", pp($removed));
  }
  if (@{$added}) {
    $self->info("Added metadata AVUs to '$path': ", pp($added));
  }
  if (@{$retained}) {
    $self->debug("Retained metadata AVUs on '$path': ", pp($retained));
  }

  return $self;
}

=head2 expected_groups

  Arg [1]    : None

  Example    : @groups = $path->expected_groups
  Description: Return a list of iRODS group names. The list might be empty or
               contain either one or multiple iRODS group names.

               An empty list is returned if the concent has been withdrawn or
               for split-out xa-human data, for which the consent does not
               exist by definition, or for split-out human data that is
               associated with multiple studies.

               Special study-related 'human' group ss_<STUDY_ID>_human is
               returned for split-out human data associated with a single study.

               In all other cases if data is associated with a list of studies,
               a list of groups is returned, a group per study, the group name
               pattern being ss_<STUDY_ID>.

               The logic is based on examining such iRODS metadata as
               'study_id', 'alignment_filter', 'sample_consent',
               'sample_consent_withdrawn'. Neither object's path nor file name
               is considered.
       
  Returntype : List

=cut

sub expected_groups {
  my ($self) = @_;

  my @af_avus = $self->find_in_metadata($ALIGNMENT_FILTER);
  my @ss_study_avus = $self->find_in_metadata($STUDY_ID);
  my $human_subset   = any { $_->{value} eq 'human' }   @af_avus;
  my $xahuman_subset = any { $_->{value} eq 'xahuman' } @af_avus;

  my $info;
  my $true  = 1;
  my $false = 0;
  my @groups;
  if ($self->get_avu($SAMPLE_CONSENT,          $false) ||
      $self->get_avu($SAMPLE_CONSENT_WITHDRAWN, $true)) {
    $info = 'Data is marked as CONSENT WITHDRAWN';
  } elsif ($xahuman_subset) {
    $info = 'Data belongs to xahuman subset';
  } elsif ($human_subset && (@ss_study_avus > 1)) {
    $info = 'Data belongs to human subset and multiple studies';
  } else {
    @groups = map { $self->irods->make_group_name($_) }
              map { $_->{value} }
              @ss_study_avus;
    if (@groups == 1 and $human_subset) {
      $self->info('Data belongs to human subset');
      @groups = ($groups[0] . '_human'); # Reset the list
    }
  }

  if ($info) {
    $self->info("${info}:\n no study-associated iRODS groups are applicable");
  }

  return @groups;
}

=head2 meta_str

  Arg [1]    : None

  Example    : $str = $path->meta_str
  Description: Synonym for meta_json.
  Returntype : Str

=cut

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

sub _update_multivalue_avus {
  my ($self, $attribute, $values, $units, $timestamp) = @_;

  defined $attribute or
    $self->logcroak("An attribute argument is required");
  defined $values or
    $self->logcroak("A defined values argument is required");

  $attribute eq q{} and
    $self->logcroak('A non-empty attribute argument is required');
  ref $values eq 'ARRAY' or
    $self->logcroak("The values argument must be an ArrayRef");

  my $path = $self->str;

  my @values = @$values;
  $self->debug("Updating all '$attribute' AVUs on '$path'");

  my @old_avus = $self->find_in_metadata($attribute);
  my @new_avus = map { $self->make_avu($attribute, $_, $units) } @values;

  # Compare old AVUS to new; if any of the new ones are already
  # present, leave the old copy, otherwise remove the old AVU
  my $num_old = scalar @old_avus;
  $self->debug("Found $num_old existing '$attribute' AVUs on $path''");

  my $history_avu;
  # Only make a history if there are some AVUs with this attribute
  if (!$self->irods->is_avu_history_attr($attribute) && $num_old > 0) {
    $history_avu = $self->make_avu_history($attribute, $timestamp);
  }

  my $num_old_processed = 0;
  my $num_old_removed   = 0;
  my @retained_avus;
  my @removed_avus;
  foreach my $old_avu (@old_avus) {
    $num_old_processed++;

    if (any { $self->avus_equal($old_avu, $_) } @new_avus) {
      $self->debug("Not updating (retaining) old AVU ",
                   $self->avu_str($old_avu), " on '$path' ",
                   "[$num_old_processed / $num_old]");
      push @retained_avus, $old_avu;
    }
    else {
      $self->debug("Updating (removing) old AVU ",
                   $self->avu_str($old_avu), " from '$path' ",
                   "[$num_old_processed / $num_old]");
      $self->remove_avu($old_avu->{attribute},
                        $old_avu->{value},
                        $old_avu->{units});
      push @removed_avus, $old_avu;
      $num_old_removed++;
    }
  }

  # Add the new AVUs, unless they are identical to one of the old
  # copies that were retained
  my $num_new = scalar @new_avus;
  $self->debug("Adding $num_new '$attribute' AVUs to '$path'");

  my $num_new_processed = 0;
  my $num_new_added     = 0;
  my @added_avus;
  foreach my $new_avu (@new_avus) {
    $num_new_processed++;

    if (any { $self->avus_equal($new_avu, $_) } @retained_avus) {
      $self->debug("Updating (using retained) new AVU ",
                   $self->avu_str($new_avu), " on '$path' ",
                   "[$num_new_processed / $num_new]");
    }
    else {
      # If we can't re-use a retained AVU, we must add this one
      $self->debug("Updating (adding) new AVU ",
                   $self->avu_str($new_avu), " to '$path' ",
                   "[$num_new_processed / $num_new]");
      push @added_avus, $new_avu;
      $self->add_avu($new_avu->{attribute},
                     $new_avu->{value},
                     $new_avu->{units});
      $num_new_added++;
    }
  }

  # Only add history if some AVUs were removed or added
  if (($num_old_removed > 0 || $num_new_added > 0) && defined $history_avu) {
    $self->debug("Adding history AVU ", $self->avu_str($history_avu),
                 " to '$path'");
    push @added_avus, $history_avu;
    $self->add_avu($history_avu->{attribute}, $history_avu->{value});
  }

  return (\@added_avus, \@removed_avus, \@retained_avus);
}

no Moose::Role;

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

Copyright (C) 2013, 2014, 2015, 2016, 2023, 2024 Genome Research Limited. All
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
