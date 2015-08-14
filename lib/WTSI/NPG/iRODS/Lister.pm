
package WTSI::NPG::iRODS::Lister;

use File::Spec;
use Moose;

our $VERSION = '';

extends 'WTSI::NPG::iRODS::Communicator';

has '+executable' => (default => 'baton-list');

##no critic (ValuesAndExpressions::ProhibitMagicNumbers)
##no critic (ValuesAndExpressions::RequireNumberSeparators)
# iRODS error code for non-existence
our $ITEM_DOES_NOT_EXIST = -310000;
##use critic

around [qw(is_collection is_object
           list_collection list_object
           list_object_checksum
           get_collection_acl get_object_acl)] => sub {
  my ($orig, $self, @args) = @_;

  unless ($self->started) {
    $self->logconfess('Attempted to use a WTSI::NPG::iRODS::Lister ',
                      'without starting it');
  }

  return $self->$orig(@args);
};

sub is_object {
  my ($self, $path) = @_;

  my $response = $self->_list_path($path);
  my $is_object;

  if (exists $response->{error}) {
    if ($response->{error}->{code} == $ITEM_DOES_NOT_EXIST) {
      # Continue to return undef
    }
    else {
      $self->report_error($response);
    }
  }
  else {
    if (exists $response->{data_object}) {
      $is_object = 1;
    }
    else {
      $is_object = 0;
    }
  }

  return $is_object;
}

sub is_collection {
  my ($self, $path) = @_;

  my $response = $self->_list_path($path);
  my $is_collection;

  if (exists $response->{error}) {
    if ($response->{error}->{code} == $ITEM_DOES_NOT_EXIST) {
      # Continue to return undef
    }
    else {
      $self->report_error($response);
    }
  }
  else {
    if (exists $response->{data_object}) {
      $is_collection = 0;
    }
    else {
      $is_collection = 1;
    }
  }

  return $is_collection;
}

=head2 list_object

  Arg [1]    : iRODS data object path.

  Example    : my $path = $irods->list_object('/path/to/object')
  Description: Return a fully qualified iRODS data object path.
  Returntype : Str

=cut

sub list_object {
  my ($self, $object) = @_;

  my $response = $self->_list_path($object);
  my $path;

  if (exists $response->{error}) {
    if ($response->{error}->{code} == $ITEM_DOES_NOT_EXIST) {
      # Continue to return undef
    }
    else {
      $self->report_error($response);
    }
  }
  else {
    $path = $self->path_spec_str($response);
  }

  return $path;
}

sub list_object_checksum {
  my ($self, $object) = @_;

  my $response = $self->_list_path($object);
  my $checksum;

  if (exists $response->{error}) {
    if ($response->{error}->{code} == $ITEM_DOES_NOT_EXIST) {
      # Continue to return undef
    }
    else {
      $self->report_error($response);
    }
  }
  else {
    $checksum = $response->{checksum};
  }

  return $checksum;
}

=head2 list_collection

  Arg [1]    : iRODS collection path.
  Arg [2]    : Recursive list flag (optional).

  Example    : my $path = $irods->list_object('/path/to/object')
  Description: Return an array of three values; the first being an
               ArrayRef of contained data objects, the second being
               an ArrayRef of contained collections, the third a HashRef
               mapping of the contained data object paths to their checksums.
  Returntype : ArrayRef[Str], ArrayRef[Str], HashRef[Str]

=cut

sub list_collection {
  my ($self, $collection, $recur) = @_;

  my $obj_specs;
  my $coll_specs;

  if ($recur) {
    ($obj_specs, $coll_specs) = $self->_list_collection_recur($collection);
  }
  else {
    ($obj_specs, $coll_specs) = $self->_list_collection($collection);
  }

  my @paths;
  if ($obj_specs and $coll_specs) {
    my @data_objects = map { $self->path_spec_str($_) } @$obj_specs;
    my @collections  = map { $self->path_spec_str($_) } @$coll_specs;
    my %checksums    = map { $self->path_spec_str($_) =>
                             $self->path_spec_checksum($_) } @$obj_specs;
    @paths = (\@data_objects, \@collections, \%checksums);
  }

  return @paths;
}

=head2 get_object_acl

  Arg [1]    : iRODS data object path.

  Example    : my @acls = $irods->get_object_acl('/path/to/object')
  Description: Return the ACLs of a data object. Each element in the ACL
               is represented as a HashRef with keys 'owner' and 'level'
               whose values are the iRODS user/group and iRODS access
               level, respectively.
  Returntype : Array[HashRef[Str]]

=cut

sub get_object_acl {
  my ($self, $object) = @_;

  my $response = $self->_list_path($object);
  my $acl;

  if (exists $response->{error}) {
    if ($response->{error}->{code} == $ITEM_DOES_NOT_EXIST) {
      # Continue to return undef
    }
    else {
      $self->report_error($response);
    }
  }
  else {
    $acl = $self->_to_acl($response);
  }

  $self->debug("ACL of '$object' is ", $self->_to_acl_str($acl));

  return @$acl;
}

=head2 get_collection_acl

  Arg [1]    : iRODS collection path.

  Example    : my @acls = $irods->get_collection_acl('/path/to/collection')
  Description: Return the ACLs of a collection. Each element in the ACL
               is represented as a HashRef with keys 'owner' and 'level'
               whose values are the iRODS user/group and iRODS access
               level, respectively.
  Returntype : Array[HashRef[Str]]

=cut

sub get_collection_acl {
  my ($self, $collection) = @_;

  my ($object_specs, $collection_specs) = $self->_list_collection($collection);

  my @acl;
  if ($collection_specs) {
    my $collection_spec = shift @$collection_specs;

    my $acl = $self->_to_acl($collection_spec);
    $self->debug("ACL of '$collection' is ", $self->_to_acl_str($acl));

    push @acl, @$acl;
  }

  return @acl;
}

sub _list_path {
  my ($self, $path) = @_;

  defined $path or
    $self->logconfess('A defined path argument is required');

  $path =~ m{^/}msx or
    $self->logconfess("An absolute path argument is required: ",
                      "received '$path'");

  my ($volume, $collection, $data_name) = File::Spec->splitpath($path);
  $collection = File::Spec->canonpath($collection);

  my $spec = {collection  => $collection,
              data_object => $data_name};
  my $response = $self->communicate($spec);
  $self->validate_response($response);

  return $response;
}

sub _list_collection {
  my ($self, $collection) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection =~ m{^/}msx or
    $self->logconfess("An absolute collection path argument is required: ",
                      "received '$collection'");
  $collection = File::Spec->canonpath($collection);

  my $spec = {collection => $collection};
  my $response = $self->communicate($spec);
  $self->validate_response($response);
  # $self->report_error($response);

  my @all_specs;

  if ($response->{error} &&
      $response->{error}->{code} == $ITEM_DOES_NOT_EXIST) {
    # Return empty @all_specs;
  }
  else {
    my @object_specs;
    my @collection_specs;

    if (!exists $response->{contents}) {
      $self->logconfess('The returned path spec did not have ',
                        'a "contents" key: ', $self->encode($response));
    }

    my @contents = @{delete $response->{contents}};
    push @collection_specs, $response;

    foreach my $path (@contents) {
      if (exists $path->{data_object}) {
        push @object_specs, $path;
      }
      else {
        push @collection_specs, $path;
      }
    }

    @all_specs = (\@object_specs, \@collection_specs);
  }

  return @all_specs;
}

# Return two arrays of path specs, given a collection path to recurse
sub _list_collection_recur {
  my ($self, $collection) = @_;

  $self->debug("Recursing into '$collection'");
  my ($obj_specs, $coll_specs) = $self->_list_collection($collection);

  my @coll_specs = @$coll_specs;
  my $this_coll = shift @coll_specs;

  my @all_obj_specs  = @$obj_specs;
  my @all_coll_specs = ($this_coll);

  foreach my $sub_coll (@coll_specs) {
    my $path = $self->path_spec_str($sub_coll);
    $self->debug("Recursing into sub-collection '$path'");

    my ($sub_obj_specs, $sub_coll_specs) = $self->_list_collection_recur($path);
    push @all_obj_specs,  @$sub_obj_specs;
    push @all_coll_specs, @$sub_coll_specs;
  }

  return (\@all_obj_specs, \@all_coll_specs);
}

sub _to_acl {
  my ($self, $path_spec) = @_;

  defined $path_spec or
    $self->logconfess('A defined path_spec argument is required');

  ref $path_spec eq 'HASH' or
    $self->logconfess('A HashRef path_spec argument is required');

  exists $path_spec->{access} or
    $self->logconfess('The path_spec argument did not have an "access" key');

  return $path_spec->{access};
}

sub _to_acl_str {
  my ($self, $acl) = @_;

  defined $acl or
    $self->logconfess('A defined acl argument is required');

  ref $acl eq 'ARRAY' or
    $self->logconfess('An ArrayRef acl argument is required');

  my $str = '[';

  my @strs;
  foreach my $elt (@$acl) {
    push @strs, sprintf "%s:%s", $elt->{owner}, $elt->{level};
  }

  return '[' . join(', ', @strs) . ']' ;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::Lister

=head1 DESCRIPTION

A client that lists iRODS data objects and collections as JSON.

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
