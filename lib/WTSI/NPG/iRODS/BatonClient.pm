package WTSI::NPG::iRODS::BatonClient;

use namespace::autoclean;

use Data::Dump qw(pp);
use File::Basename qw(fileparse);
use JSON;
use Moose;
use MooseX::StrictConstructor;

our $VERSION = '';

extends 'WTSI::NPG::iRODS::Communicator';

has '+executable' => (default => 'baton-do');

##no critic (ValuesAndExpressions::ProhibitMagicNumbers)
##no critic (ValuesAndExpressions::RequireNumberSeparators)
# iRODS error code for non-existence
our $ITEM_DOES_NOT_EXIST = -310000;

around 'communicate' => sub {
  my ($orig, $self, @args) = @_;

  my $envelope = $self->$orig(@args);

  my $unwrapped;
  if (exists $envelope->{result}) {
    if (exists $envelope->{result}->{single}) {
      $unwrapped = $envelope->{result}->{single}
    }
    elsif (exists $envelope->{result}->{multiple}) {
      $unwrapped = $envelope->{result}->{multiple}
    }
  }
  elsif (exists $envelope->{target}) {
    $unwrapped = $envelope->{target}
  }

  if (not $unwrapped) {
    $self->logconfess('Failed to unwrap the baton JSON envelope: ',
                      pp($envelope));
  }

  # Copy any envelope error report into the response
  if (exists $envelope->{error}) {
    $unwrapped->{error} = $envelope->{error};
  }

  return $unwrapped;
};

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

sub put_object {
  my ($self, $local_path, $remote_path, $checksum) = @_;

  my ($file_name, $directory, $suffix) = fileparse($local_path);
  my ($data_object, $collection) = fileparse($remote_path);

  my $checksum_args = {};
  if ($checksum == $WTSI::NPG::iRODS::SKIP_CHECKSUM) {
    $checksum_args->{checksum} = JSON::false;
  } elsif ($checksum == $WTSI::NPG::iRODS::CALC_CHECKSUM) {
    $checksum_args->{checksum} = JSON::true;
  } elsif ($checksum == $WTSI::NPG::iRODS::VERIFY_CHECKSUM) {
    $checksum_args->{verify} = JSON::true;
  }

  my $spec = {operation => 'put',
              arguments => $self->_map_json_args($checksum_args),
              target    => {collection  => $collection,
                            data_object => $data_object,
                            directory   => $directory,
                            file        => $file_name}};

  my $response = $self->communicate($spec);
  $self->validate_response($response);
  $self->report_error($response);

  return $remote_path;
}

# This is affected by https://github.com/wtsi-npg/baton/issues/189

# sub move_object {
#   my ($self, $source_path, $dest_path) = @_;

#   my ($data_object, $collection) = fileparse($source_path);

#   my $spec = {operation => 'move',
#               arguments => {path => $dest_path},
#               target    => {collection  => $collection,
#                             data_object => $data_object}};

#   my $response = $self->communicate($spec);
#   $self->validate_response($response);
#   $self->report_error($response);

#   return $dest_path
# }

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
    ($obj_specs, $coll_specs) =
      $self->_list_collection_recur($collection);
  }
  else {
    ($obj_specs, $coll_specs) =
      $self->_list_collection($collection);
  }

  my @paths;
  if ($obj_specs and $coll_specs) {
    my @data_objects = map { $self->path_spec_str($_) } @$obj_specs;
    my @collections  = map { $self->path_spec_str($_) } @$coll_specs;
    @paths = (\@data_objects, \@collections);
  }

  return @paths;
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

=head2 list_collection_checksums

  Arg [1]    : iRODS collection path.
  Arg [2]    : Recurse, Bool. Optional, defaults to false.

  Example    : my $checksums =
                 $irods->list_collection_checksums('/path/to/collection')
  Description: Return the checksums of the data objects in a collection
               as a mapping of data object path to corresponding checksum.
               This method is present for speed.
  Returntype : HashRef

=cut

sub list_collection_checksums {
  my ($self, $collection, $recur) = @_;

  my $obj_specs;
  my $coll_specs; # Ignore

  if ($recur) {
    ($obj_specs, $coll_specs) =
      $self->_list_collection_recur($collection, {contents => 1,
                                                  checksum => 1});
  }
  else {
    ($obj_specs, $coll_specs) =
      $self->_list_collection($collection, {contents => 1,
                                            checksum => 1});
  }

  my %checksums;
  if ($obj_specs) {
    %checksums = map { $self->path_spec_str($_) =>
                       $self->path_spec_checksum($_) } @$obj_specs;
  }

  return \%checksums;
}

=head2 list_object_checksum

  Arg [1]    : iRODS data object path.

  Example    : my $checksum = $irods->list_object_checksum('/path/to/object')
  Description: Return the checksum of the data object. This method uses
               the same iRODS API as the 'ichksum' client program. Return undef
               if no checksum has been calculated.
  Returntype : Str

=cut

sub list_object_checksum {
  my ($self, $object) = @_;

  my $response = $self->_list_path($object, {checksum => 1});
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

sub calculate_object_checksum {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object =~ m{^/}msx or
    $self->logconfess("An absolute object path argument is required: ",
                      "received '$object'");

  my ($volume, $collection, $data_name) = File::Spec->splitpath($object);
  $collection = File::Spec->canonpath($collection);

  $data_name or $self->logconfess("An object path argument is required: ",
                                  "received '$object'");

  my $spec = {operation => 'checksum',
              arguments => {checksum => JSON::true},
              target    => {collection  => $collection,
                            data_object => $data_name}};
  my $response = $self->communicate($spec);

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

=head2 list_object_size

  Arg [1]    : iRODS data object path.

  Example    : my size = $irods->list_object_size('/path/to/object')
  Description: Return the size of the data object. This method returns the
               value from the iRODS catalog, not the size on disk.
  Returntype : Int

=cut

sub list_object_size {
  my ($self, $object) = @_;

  my $response = $self->_list_path($object, {size => 1});
  my $size;

  if (exists $response->{error}) {
    if ($response->{error}->{code} == $ITEM_DOES_NOT_EXIST) {
      # Continue to return undef
    }
    else {
      $self->report_error($response);
    }
  }
  else {
    $size = $response->{size};
  }

  return $size;
}

=head2 list_object_replicates

  Arg [1]    : iRODS data object path.

  Example    : my @replicates =
                 $irods->list_object_replicates('/path/to/object')
  Description: Return the replicates of the data object. Each replicate
               is represented as a HashRef of the form:
                   {
                     checksum => <checksum Str>,
                     location => <location Str>,
                     number   => <replicate number Int>,
                     resource => <resource name Str>,
                     valid    => <is valid Int>,
                   }

                The checksum of each replicate is reported using the iRODS
                GenQuery API.
  Returntype : Array[HashRef]

=cut

sub list_object_replicates {
  my ($self, $object) = @_;

  my $response = $self->_list_path($object, {replicate => 1});
  my @replicates;

  if (exists $response->{error}) {
    if ($response->{error}->{code} == $ITEM_DOES_NOT_EXIST) {
      # Continue to return an empty array
    }
    else {
      $self->report_error($response);
    }
  }
  else {
    if (ref $response->{replicates} eq 'ARRAY') {
      @replicates = @{$response->{replicates}};
    }
    else {
      $self->logconfess('The returned path spec did not have a "replicates" ',
                        'key with an ArrayRef value: ',
                        $self->encode($response));
    }
  }

  # Ensure the Bool property 'valid' is a Perl boolean, not a
  # JSON::XS::Boolean proxy (which upsets Moose).
  foreach my $rep (@replicates) {
    $rep->{valid} = int $rep->{valid};
  }

  return @replicates;
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

  my ($object_specs, $collection_specs) =
    $self->_list_collection($collection, {acl => 1});

  my @acl;
  if ($collection_specs) {
    my $collection_spec = shift @$collection_specs;

    my $acl = $self->_to_acl($collection_spec);
    $self->debug("ACL of '$collection' is ", $self->_to_acl_str($acl));

    push @acl, @$acl;
  }

  return @acl;
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

  my $response = $self->_list_path($object, {acl => 1});
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

sub chmod_collection {
  my ($self, $permission, $owner, $collection) = @_;

  defined $permission or
    $self->logconfess('A defined permission argument is required');
  defined $owner or
    $self->logconfess('A defined owner argument is required');
  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection =~ m{^/}mxs or
    $self->logconfess("An absolute collection path argument is required: ",
                      "received '$collection'");

  $collection = File::Spec->canonpath($collection);

  my ($name, $zone) = split /\#/msx, $owner;
  $self->debug("Parsed owner name '$name' from '$owner' for '$collection'");

  my $perm = {owner => $name,
              level => $permission};
  if ($zone) {
    $self->debug("Parsed owner zone '$zone' from '$owner' for '$collection'");
    $perm->{zone} = $zone;
  }

  my $spec = {operation => 'chmod',
              arguments => {},
              target    => {collection => $collection,
                            access     => [$perm]}};
  my $response = $self->communicate($spec);
  $self->validate_response($response);
  $self->report_error($response);

  return $collection;
}

sub chmod_object {
  my ($self, $permission, $owner, $object) = @_;

  defined $permission or
    $self->logconfess('A defined permission argument is required');
  defined $owner or
    $self->logconfess('A defined owner argument is required');
  defined $object or
    $self->logconfess('A defined object argument is required');

  $object =~ m{^/}msx or
      $self->logconfess("An absolute object path argument is required: ",
                        "received '$object'");

  my ($volume, $collection, $data_name) = File::Spec->splitpath($object);
  $collection = File::Spec->canonpath($collection);

  my ($name, $zone) = split /\#/msx, $owner;
  $self->debug("Parsed owner name '$name' from '$owner' for '$object'");

  my $perm = {owner => $name,
              level => $permission};
  if ($zone) {
    $self->debug("Parsed owner zone '$zone' from '$owner' for '$object'");
    $perm->{zone} = $zone;
  }

  my $spec = {operation => 'chmod',
              arguments => {},
              target    => {collection  => $collection,
                            data_object => $data_name,
                            access      => [$perm]}};
  my $response = $self->communicate($spec);
  $self->validate_response($response);
  $self->report_error($response);

  return $object;
}

sub list_collection_meta {
  my ($self, $collection) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection =~ m{^/}msx or
    $self->logconfess("An absolute object path argument is required: ",
                      "received '$collection'");

  $collection = File::Spec->canonpath($collection);

  my $spec = {operation => 'list',
              arguments => $self->_map_json_args({avu => 1}),
              target    => {collection => $collection}};

  return $self->_list_path_meta($spec);
}

sub list_object_meta {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object =~ m{^/}msx or
    $self->logconfess("An absolute object path argument is required: ",
                      "received '$object'");

  my ($volume, $collection, $data_name) = File::Spec->splitpath($object);
  $collection = File::Spec->canonpath($collection);

  $data_name or $self->logconfess("An object path argument is required: ",
                                  "received '$object'");

  my $spec = {operation => 'list',
              arguments => $self->_map_json_args({avu => 1}),
              target    => {collection  => $collection,
                            data_object => $data_name}};

  return $self->_list_path_meta($spec);
}

sub list_path_details {
  my ($self, $path) = @_;

  defined $path or
    $self->logconfess('A defined path argument is required');
  $path =~ m{^/}msx or
      $self->logconfess("An absolute path argument is required: ",
                        "received '$path'");

  my $args = { acl => 1, avu => 1, replicate => 1, timestamp => 1 };
  my $response = $self->_list_path($path, $args);

  if (exists $response->{error}) {
    $self->report_error($response);
  }

  return $response;
}

sub add_collection_avu {
  my ($self, $collection, $attribute, $value, $units) = @_;

  return $self->_modify_collection_meta
    ($collection, $attribute, $value, $units, {operation => 'add'});
}

sub add_object_avu {
  my ($self, $object, $attribute, $value, $units) = @_;

  return $self->_modify_object_meta
    ($object, $attribute, $value, $units, {operation => 'add'});
}

sub remove_collection_avu {
  my ($self, $collection, $attribute, $value, $units) = @_;

  return $self->_modify_collection_meta
    ($collection, $attribute, $value, $units, {operation => 'rem'});
}

sub remove_object_avu {
  my ($self, $object, $attribute, $value, $units) = @_;

  return $self->_modify_object_meta
    ($object, $attribute, $value, $units, {operation => 'rem'});
}

sub search_collections {
  my ($self, $zone_hint, @avus) = @_;

  return $self->_search($zone_hint, \@avus, {collection => 1});
}

sub search_objects {
  my ($self, $zone_hint, @avus) = @_;

  return $self->_search($zone_hint, \@avus, {object => 1});
}

=head2 read_object

  Arg [1]    : Data object absolute path

  Example    : $reader->read_object('/path/to/object.txt')
  Description: Read UTF-8 content from a data object.
  Returntype : Str

=cut

sub read_object {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object =~ m{^/}msx or
    $self->logconfess("An absolute object path argument is required: ",
                      "received '$object'");

  my ($volume, $collection, $data_name) = File::Spec->splitpath($object);
  $collection = File::Spec->canonpath($collection);

  my $spec = {operation => 'get',
              arguments => {},
              target    => {collection  => $collection,
                            data_object => $data_name}};

  my $response = $self->communicate($spec);

  $self->validate_response($response);
  $self->report_error($response);

  if (!exists $response->{data}) {
    $self->logconfess('The returned path spec did not have a "data" key: ',
                      $self->encode($response));
  }

  return $response->{data};
}


=head2 remove_collection_safely

  Arg [1]    : Collection absolute path

  Example    : $irods->remove_collection_safely('/path/to/collection')
  Description: Remove a collection if it is empty. Raise an error if it is not
               empty.
  Returntype : None

=cut

sub remove_collection_safely {
  my ($self, $collection, $recurse) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection =~ m{^/}mxs or
    $self->logconfess("An absolute collection path argument is required: ",
                      "received '$collection'");

  $collection = File::Spec->canonpath($collection);

  if ($recurse) {
    $self->_remove_collection_recur($collection);
  } else {
    my $spec = { operation => 'rmdir',
                 arguments => {},
                 target    => { collection => $collection } };
    my $response = $self->communicate($spec);
    $self->validate_response($response);
    $self->report_error($response);
  }

  return;
}

sub _search {
  my ($self, $zone_hint, $avus, $args) = @_;

  defined $zone_hint or
    $self->logconfess('A defined zone_hint argument is required');

  my $i = 0;
  foreach my $avu (@{$avus}) {
    unless (ref $avu eq 'HASH') {
      $self->logconfess("A query AVU must be a HashRef: AVU #$i was not");
    }

    my $astr = defined $avu->{attribute} ? $avu->{attribute} : 'undef';
    my $vstr = defined $avu->{value}     ? $avu->{value}     : 'undef';
    my $ustr = defined $avu->{units}     ? $avu->{units}     : 'undef';

    # Zero length keys and values are rejected by iRODS
    unless (length $avu->{attribute}) {
      $self->logconfess("A query AVU must have an attribute: ",
                        "AVU #$i did not: {$astr, $vstr, $ustr}");
    }
    unless (length $avu->{value}) {
      $self->logconfess("A query AVU must have a value: ",
                        "AVU #$i did not: {$astr, $vstr, $ustr}");
    }

    $i++;
  }

  my $spec = {operation => 'metaquery',
              arguments => $self->_map_json_args($args),
              target    => {collection => $zone_hint,
                            avus       => $avus}};

  my $response = $self->communicate($spec);
  $self->_validate_response($response);
  $self->report_error($response);

  my @results =  map { $self->path_spec_str($_) } @$response;

  return \@results;
}

sub _validate_response {
  my ($self, $response) = @_;

  # The ony valid response is a HashRef or ArrayRef
  my $rtype = ref $response;
  unless ($rtype eq 'HASH' || $rtype eq 'ARRAY') {
    $self->logconfess("Failed to get a HashRef or Array response; got $rtype");
  }

  return $self;
}

sub _modify_collection_meta {
  my ($self, $collection, $attribute, $value, $units, $args) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection =~ m{^/}msx or
    $self->logconfess("An absolute collection path argument is required: ",
                      "received '$collection'");

  defined $attribute or
    $self->logconfess('A defined attribute argument is required');
  defined $value or
    $self->logconfess('A defined value argument is required');

  $collection = File::Spec->canonpath($collection);

  my $spec = {operation => 'metamod',
              arguments => $args,
              target    => {collection => $collection,
                            avus       => [{attribute => $attribute,
                                            value     => $value}]}};
  if ($units) {
    $spec->{target}->{avus}->[0]->{units} = $units;
  }

  my $response = $self->communicate($spec);
  $self->validate_response($response);
  $self->report_error($response);

  return $collection;
}

sub _modify_object_meta {
  my ($self, $object, $attribute, $value, $units, $args) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object =~ m{^/}msx or
    $self->logconfess("An absolute object path argument is required: ",
                      "received '$object'");

  defined $attribute or
    $self->logconfess('A defined attribute argument is required');
  defined $value or
    $self->logconfess('A defined value argument is required');

  my ($volume, $collection, $data_name) = File::Spec->splitpath($object);
  $collection = File::Spec->canonpath($collection);

  $data_name or $self->logconfess("An object path argument is required: ",
                                  "received '$object'");

  my $spec = {operation => 'metamod',
              arguments => $args,
              target    => {collection  => $collection,
                            data_object => $data_name,
                            avus        => [{attribute => $attribute,
                                             value     => $value}]}};
  if ($units) {
    $spec->{target}->{avus}->[0]->{units} = $units;
  }

  my $response = $self->communicate($spec);
  $self->validate_response($response);
  $self->report_error($response);

  return $object;
}

sub _list_path {
  my ($self, $path, $args) = @_;

  defined $path or
    $self->logconfess('A defined path argument is required');

  $path =~ m{^/}msx or
    $self->logconfess("An absolute path argument is required: ",
                      "received '$path'");

  $args ||= {};

  my ($volume, $collection, $data_name) = File::Spec->splitpath($path);
  $collection = File::Spec->canonpath($collection);

  my $spec = {operation => 'list',
              arguments => $self->_map_json_args($args),
              target    => {collection  => $collection,
                            data_object => $data_name}};
  my $response = $self->communicate($spec);
  $self->validate_response($response);

  return $response;
}

sub _list_collection {
  my ($self, $collection, $args) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection =~ m{^/}msx or
    $self->logconfess("An absolute collection path argument is required: ",
                      "received '$collection'");
  $collection = File::Spec->canonpath($collection);

  $args ||= {};
  $args->{contents} = 1;

  my $spec =  {operation => 'list',
               arguments => $self->_map_json_args($args),
               target    => {collection => $collection}};
  my $response = $self->communicate($spec);
  $self->validate_response($response);

  my @all_specs;

  if ($response->{error} &&
      $response->{error}->{code} == $ITEM_DOES_NOT_EXIST) {
    @all_specs = (undef, undef);
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
  my ($self, $collection, $args) = @_;

  $self->debug("Recursing into '$collection'");
  my ($obj_specs, $coll_specs) = $self->_list_collection($collection, $args);

  my @coll_specs = @$coll_specs;
  my $this_coll = shift @coll_specs;

  my @all_obj_specs  = @$obj_specs;
  my @all_coll_specs = ($this_coll);

  foreach my $sub_coll (@coll_specs) {
    my $path = $self->path_spec_str($sub_coll);
    $self->debug("Recursing into sub-collection '$path'");

    my ($sub_obj_specs, $sub_coll_specs) =
      $self->_list_collection_recur($path, $args);
    push @all_obj_specs,  @$sub_obj_specs;
    push @all_coll_specs, @$sub_coll_specs;
  }

  return (\@all_obj_specs, \@all_coll_specs);
}

sub _remove_collection_recur {
  my ($self, $collection) = @_;

  $self->debug("Recursing into '$collection'");
  my ($obj_specs, $coll_specs) = $self->_list_collection($collection);

  my @coll_specs = @$coll_specs;
  my $this_coll = shift @coll_specs;

  foreach my $sub_coll (@coll_specs) {
    my $path = $self->path_spec_str($sub_coll);
    $self->debug("Recursing into sub-collection '$path'");
    $self->_remove_collection_recur($path);
  }

  my $path = $self->path_spec_str($this_coll);
  $self->remove_collection_safely($path);

  return;
}

sub _list_path_meta {
  my ($self, $spec) = @_;

  defined $spec or
    $self->logconfess('A defined JSON spec argument is required');

  my $response = $self->communicate($spec);
  $self->validate_response($response);
  $self->report_error($response);

  my @avus;
  if (ref $response->{avus} eq 'ARRAY') {
    @avus = @{$response->{avus}};
  }
  else {
    $self->logconfess('The returned path spec did not have an "avus" key ',
                      'with an ArrayRef value: ', $self->encode($response));
  }

  return @avus;
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

sub _map_json_args {
  my ($self, $args) = @_;

  defined $args or
    $self->logconfess('A defined args argument is required');

  ref $args eq 'HASH' or
    $self->logconfess('The args argument must be a HashRef: ',
                      'recieved ', ref $args);

  my $mapped = {};
  foreach my $arg (keys %{$args}) {
    $mapped->{$arg} = $args->{$arg} ? JSON::true : JSON::false;
  }

  return $mapped;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::BatonClient

=head1 DESCRIPTION

A wrapper for the baton-do command line iRODS client.

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2013, 2014, 2015, 2017, 2019, 2021 Genome Research
Limited. All Rights Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
