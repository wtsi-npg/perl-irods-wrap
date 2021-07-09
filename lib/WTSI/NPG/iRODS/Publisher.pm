package WTSI::NPG::iRODS::Publisher;

use namespace::autoclean;
use Carp;
use Data::Dump qw[pp];
use DateTime;
use English qw[-no_match_vars];
use File::Basename;
use File::Spec::Functions qw[catdir catfile splitdir splitpath];
use File::stat;
use List::MoreUtils qw[any];
use Moose;
use Try::Tiny;

use WTSI::NPG::iRODS::Collection;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::iRODS;

our $VERSION = '';

with qw[
         WTSI::DNAP::Utilities::Loggable
         WTSI::NPG::Accountable
         WTSI::NPG::iRODS::AVUCollator
         WTSI::NPG::iRODS::Annotator
];

has 'irods' =>
  (is            => 'ro',
   isa           => 'WTSI::NPG::iRODS',
   required      => 1,
   lazy          => 1,
   builder       => '_build_irods',
   documentation => 'The iRODS connection handle');

has 'checksum_cache_threshold' =>
  (is            => 'ro',
   isa           => 'Int',
   required      => 1,
   default       => 2048,
   documentation => 'The size above which file checksums will be cached');

has 'require_checksum_cache' =>
  (is            => 'ro',
   isa           => 'ArrayRef[Str]',
   required      => 1,
   default       => sub { return [qw[bam cram]] },
   documentation => 'A list of file suffixes for which MD5 cache files ' .
                    'must be provided and will not be created on the fly');

has 'checksum_cache_time_delta' =>
  (is            => 'rw',
   isa           => 'Int',
   required      => 1,
   default       => 60,
   documentation => 'Time delta in seconds for checksum cache files to be ' .
                    'considered stale. If a data file is newer than its '   .
                    'cache by more than this number of seconds, the cache ' .
                    'is stale');


=head2 publish

  Arg [1]    : Path to local file for directory, Str.
  Arg [2]    : Path to destination in iRODS, Str.
  Arg [3]    : Custom metadata AVUs to add, ArrayRef[HashRef].
  Arg [4]    : Timestamp to use in metadata, DateTime. Optional, defaults
               to current time.

  Example    : my $path = $pub->publish('./local/file.txt',
                                        '/zone/path/file.txt',
                                        [{attribute => 'x',
                                          value     => 'y'}])
  Description: Publish a local file or directory to iRODS, detecting which
               has been passed as an argument and then delegating to
               'publish_file' or 'publish_directory' as appropriate.
  Returntype : WTSI::NPG::iRODS::DataObject or WTSI::NPG::iRODS::Collection

=cut

sub publish {
  my ($self, $local_path, $remote_path, $metadata, $timestamp) = @_;

  my $published;
  if (-f $local_path) {
    $published = $self->publish_file($local_path, $remote_path, $metadata,
                                     $timestamp);
  }
  elsif (-d $local_path) {
    $published = $self->publish_directory($local_path, $remote_path, $metadata,
                                          $timestamp);
  }
  else {
    $self->logconfess('The local_path argument as neither a file nor a ',
                      'directory: ', "'$local_path'");
  }

  return $published;
}

=head2 publish_file

  Arg [1]    : Path to local file, Str.
  Arg [2]    : Path to destination in iRODS, Str.
  Arg [3]    : Custom metadata AVUs to add, ArrayRef[HashRef].
  Arg [4]    : Timestamp to use in metadata, DateTime. Optional, defaults
               to current time.

  Example    : my $obj = $pub->publish_file('./local/file.txt',
                                            '/zone/path/file.txt',
                                            [{attribute => 'x',
                                              value     => 'y'}])
  Description: Publish a local file to iRODS, create and/or supersede
               metadata (both default and custom) and update permissions,
               returning the absolute path of the published data object.

               If the target path does not exist in iRODS the file will
               be transferred. Default creation metadata will be added and
               custom metadata will be added.

               If the target path exists in iRODS, the checksum of the
               local file will be compared with the cached checksum in
               iRODS. If the checksums match, the local file will not
               be uploaded. Default modification metadata will be added
               and custom metadata will be superseded.

               In both cases, permissions will be updated.
  Returntype : WTSI::NPG::iRODS::DataObject

=cut

sub publish_file {
  my ($self, $local_path, $remote_path, $metadata, $timestamp) = @_;

  $self->_check_path_args($local_path, $remote_path);
  -f $local_path or
    $self->logconfess("The local_path argument '$local_path' was not a file");

  if (defined $metadata and ref $metadata ne 'ARRAY') {
    $self->logconfess('The metadata argument must be an ArrayRef');
  }
  if (not defined $timestamp) {
    $timestamp = DateTime->now;
  }

  my $obj;
  if ($self->irods->is_collection($remote_path)) {
    $self->info("Remote path '$remote_path' is a collection");

    my ($loc_vol, $dir, $file) = splitpath($local_path);
    $obj = $self->publish_file($local_path, catfile($remote_path, $file),
                               $metadata, $timestamp)
  }
  else {
    my $local_md5 = $self->_get_md5($local_path);
    if ($self->irods->is_object($remote_path)) {
      $self->info("Remote path '$remote_path' is an existing object");
      $obj = $self->_publish_file_overwrite($local_path, $local_md5,
                                            $remote_path, $timestamp);
    }
    else {
      $self->info("Remote path '$remote_path' is a new object");
      $obj = $self->_publish_file_create($local_path, $local_md5,
                                         $remote_path, $timestamp);
    }

    my $num_meta_errors = $self->_supersede_multivalue($obj, $metadata);
    if ($num_meta_errors > 0) {
       $self->logcroak("Failed to update metadata on '$remote_path': ",
                       "$num_meta_errors errors encountered ",
                       '(see log for details)');
     }

  }

  return $obj;
}

=head2 publish_directory

  Arg [1]    : Path to local directory, Str.
  Arg [2]    : Path to destination in iRODS, Str.
  Arg [3]    : Custom metadata AVUs to add, ArrayRef[HashRef].
  Arg [4]    : Timestamp to use in metadata, DateTime. Optional, defaults
               to current time.

  Example    : my $coll = $pub->publish_directory('./local/dir',
                                                  '/zone/path',
                                                  [{attribute => 'x',
                                                    value     => 'y'}])
  Description: Publish a local directory to iRODS, create and/or supersede
               metadata (both default and custom) and update permissions,
               returning the absolute path of the published collection.

               The local directory will be inserted into the destination
               collection as a new sub-collection. No checks are made on the
               files with in the new collection.
  Returntype : WTSI::NPG::iRODS::Collection

=cut

sub publish_directory {
  my ($self, $local_path, $remote_path, $metadata, $timestamp) = @_;

  $self->_check_path_args($local_path, $remote_path);
  -d $local_path or
    $self->logconfess("The local_path argument '$local_path' ",
                      'was not a directory');

  if (defined $metadata and ref $metadata ne 'ARRAY') {
    $self->logconfess('The metadata argument must be an ArrayRef');
  }
  if (not defined $timestamp) {
    $timestamp = DateTime->now;
  }

  $remote_path = $self->_ensure_collection_exists($remote_path);
  my $coll_path = $self->irods->put_collection($local_path, $remote_path);
  my $coll = WTSI::NPG::iRODS::Collection->new($self->irods, $coll_path);

  my @meta = $self->make_creation_metadata($self->affiliation_uri,
                                           $timestamp,
                                           $self->accountee_uri);
  if (defined $metadata) {
    push @meta, @{$metadata};
  }

  my $num_meta_errors = $self->_supersede_multivalue($coll, \@meta);
  if ($num_meta_errors > 0) {
    $self->logcroak("Failed to update metadata on '$remote_path': ",
                    "$num_meta_errors errors encountered ",
                    '(see log for details)');
  }

  return $coll;
}

sub _build_irods {
  my ($self) = @_;

  return WTSI::NPG::iRODS->new;
}

sub _check_path_args {
  my ($self, $local_path, $remote_path) = @_;

  defined $local_path or
    $self->logconfess('A defined local_path argument is required');
  defined $remote_path or
    $self->logconfess('A defined remote_path argument is required');

  $local_path eq q[] and
    $self->logconfess('A non-empty local_path argument is required');
  $remote_path eq q[] and
    $self->logconfess('A non-empty remote_path argument is required');

  $remote_path =~ m{^/}msx or
    $self->logconfess("The remote_path argument '$remote_path' ",
                      'was not absolute');

  return;
}

sub _ensure_collection_exists {
  my ($self, $remote_path) = @_;

  my $collection;
  if ($self->irods->is_object($remote_path)) {
    $self->logconfess("The remote_path argument '$remote_path' ",
                      'was a data object');
  }
  elsif ($self->irods->is_collection($remote_path)) {
    $self->debug("Remote path '$remote_path' is a collection");
    $collection = $remote_path;
  }
  else {
    $collection = $self->irods->add_collection($remote_path);
  }

  return $collection;
}

sub _publish_file_create {
  my ($self, $local_path, $local_md5, $remote_path, $timestamp) = @_;

  $self->debug("Remote path '$remote_path' does not exist");
  my ($loc_vol, $dir, $file)      = splitpath($local_path);
  my ($rem_vol, $coll, $obj_name) = splitpath($remote_path);

  if ($file ne $obj_name) {
    $self->info("Renaming '$file' to '$obj_name' on publication");
  }

  $self->_ensure_collection_exists($coll);
  $self->info("Publishing new object '$remote_path'");

  $self->irods->add_object($local_path, $remote_path,
                           $WTSI::NPG::iRODS::VERIFY_CHECKSUM);

  my $obj = WTSI::NPG::iRODS::DataObject->new($self->irods, $remote_path);
  my $remote_md5 = $obj->checksum;

  my $num_meta_errors =
    $self->_supersede($obj,
                      $self->make_creation_metadata($self->affiliation_uri,
                                                    $timestamp,
                                                    $self->accountee_uri),
                      $self->make_md5_metadata($remote_md5),
                      $self->make_type_metadata($remote_path));

  if ($local_md5 eq $remote_md5) {
    $self->info("After publication of '$local_path' ",
                "MD5: '$local_md5' to '$remote_path' ",
                "MD5: '$remote_md5': checksums match");
  }
  else {
    # Maybe tag with metadata to identify a failure?
    $self->logcroak("After publication of '$local_path' ",
                    "MD5: '$local_md5' to '$remote_path' ",
                    "MD5: '$remote_md5': checksum mismatch");
  }

  if ($num_meta_errors > 0) {
    $self->logcroak("Failed to update metadata on '$remote_path': ",
                    "$num_meta_errors errors encountered ",
                    '(see log for details)');
  }

  return $obj;
}

sub _publish_file_overwrite {
  my ($self, $local_path, $local_md5, $remote_path, $timestamp) = @_;

  $self->info("Remote path '$remote_path' is a data object");
  my $obj = WTSI::NPG::iRODS::DataObject->new($self->irods, $remote_path);

  # Preemptively fix any missing checksums. As of iRODS 4.2.8 it is possible
  # for some data object replicates to be missing their checksum due to a
  # previous failed or interrupted upload. This method call will not update
  # any checksums already present in the catalog, but will create any that
  # are missing according to the data already on disk.
  my $pre_remote_md5 = $obj->calculate_checksum;
  my $post_remote_md5;

  my $num_meta_errors  = 0;
  my $num_write_errors = 0;

  if (not $obj->is_consistent_size) {
    my $remote_size = $obj->size;
    $self->warn("Re-publishing '$local_path' to '$remote_path' ",
      "(remote size and checksum inconsistent): local MD5 is '$local_md5', ",
      "remote is MD5: '$pre_remote_md5' and size $remote_size");
  }

  if ($local_md5 eq $pre_remote_md5 and $obj->is_consistent_size) {
    $self->info("Skipping publication of '$local_path' to '$remote_path': ",
                "(checksum unchanged): local MD5 is '$local_md5', ",
                "remote is MD5: '$pre_remote_md5'");
  }
  else {
    $self->info("Re-publishing '$local_path' to '$remote_path' ",
                "(checksum changed): local MD5 is '$local_md5', ",
                "remote is MD5: '$pre_remote_md5'");
    try {
      # Ensure that pre-update metadata are correct
      $num_meta_errors +=
        $self->_supersede($obj, $self->make_type_metadata($remote_path),
          $self->make_md5_metadata($pre_remote_md5));

      $self->irods->replace_object($local_path, $obj->str,
                                   $WTSI::NPG::iRODS::VERIFY_CHECKSUM);

      $post_remote_md5 =
        WTSI::NPG::iRODS::DataObject->new($self->irods,
                                          $remote_path)->checksum;

      # Add modification metadata only if successful
      $num_meta_errors +=
        $self->_supersede($obj,
                          $self->make_md5_metadata($post_remote_md5),
                          $self->make_modification_metadata($timestamp));
    } catch {
      $num_write_errors++;
      $self->error(q[Failed to overwrite existing data object at '],
                   $obj->str, q[' while re-publishing]);
    };

    if ($num_write_errors > 0) {
      $self->logcroak("Failed to re-publish '$local_path' to '$remote_path': ",
                      "(checksum unknown): local MD5 was '$local_md5', ",
                      'remote MD5 was unknown');
    }

    if (not defined $post_remote_md5) {
      $self->logcroak("Failed to re-publish '$local_path' to '$remote_path': ",
                      "(checksum unknown): local MD5 was '$local_md5', ",
                      'remote MD5 was unknown');
    }
    elsif ($local_md5 eq $post_remote_md5) {
      $self->info("Re-published '$local_path' to '$remote_path': ",
                  "(checksums match): local MD5 was '$local_md5', ",
                  "remote was MD5: '$pre_remote_md5', ",
                  "remote now MD5: '$post_remote_md5'");
    }
    elsif ($pre_remote_md5 eq $post_remote_md5) {
      # Maybe tag with metadata to identify a failure?
      $self->logcroak("Failed to re-publish '$local_path' to '$remote_path': ",
                      "(checksum unchanged): local MD5 was '$local_md5', ",
                      "remote was MD5: '$pre_remote_md5', ",
                      "remote now MD5: '$post_remote_md5'");
    }
    else {
      # Maybe tag with metadata to identify a failure?
      $self->logcroak("Failed to re-publish '$local_path' to '$remote_path': ",
                      "(checksum mismatch): local MD5 was '$local_md5', ",
                      "remote was MD5: '$pre_remote_md5', ",
                      "remote now MD5: '$post_remote_md5'");
    }
  }

  if ($num_meta_errors > 0) {
    $self->logcroak("Failed to update metadata on '$remote_path': ",
                    "$num_meta_errors errors encountered ",
                    '(see log for details)');
  }

  return $obj;
}

sub _supersede {
  my ($self, $item, @metadata) = @_;

  my $path = $item->str;
  $self->debug("Setting metadata on '$path': ", pp(\@metadata));

  my $num_errors = 0;
  foreach my $avu (@metadata) {
    my $attr  = $avu->{attribute};
    my $value = $avu->{value};
    my $units = $avu->{units};

    try {
      $item->supersede_avus($attr, $value, $units);
    } catch {
      $num_errors++;
      $self->error("Failed to supersede AVU on '$path' with attribute ",
                   "'$attr' and value '$value': ", $_);
    };
  }

  return $num_errors;
}

sub _supersede_multivalue {
  my ($self, $item, $metadata) = @_;

  my $path = $item->str;
  $self->debug("Setting metadata on '$path': ", pp($metadata));

  my %collated_avus = %{$self->collate_avus(@{$metadata})};

  # Sorting by attribute to allow repeated updates to be in
  # deterministic order
  my @attributes = sort keys %collated_avus;
  $self->debug("Superseding AVUs on '$path' in order of attributes: ",
               join q[, ], @attributes);

  my $num_errors = 0;
  foreach my $attr (@attributes) {
    my $values = $collated_avus{$attr};
    try {
      $item->supersede_multivalue_avus($attr, $values, undef);
    } catch {
      $num_errors++;
      $self->error("Failed to supersede AVU on '$path' with attribute '$attr' ",
                   'and values ', pp($values), q[: ], $_);
    }
  }

  return $num_errors;
}

sub _get_md5 {
  my ($self, $path) = @_;

  defined $path or $self->logconfess('A defined path argument is required');
  -e $path or $self->logconfess("The path '$path' does not exist");

  my ($suffix) = $path =~ m{[.]([^.]+)$}msx;
  my $cache_file = "$path.md5";
  my $md5 = q[];

  if (-e $cache_file and $self->_md5_cache_file_stale($path, $cache_file)) {
    $self->warn("Deleting stale MD5 cache file '$cache_file' for '$path'");
    unlink $cache_file or $self->warn("Failed to unlink '$cache_file'");
  }

  if (-e $cache_file) {
    $md5 = $self->_read_md5_cache_file($cache_file);
  }

  if (not $md5) {
    if ($suffix and any { $suffix eq $_ } @{$self->require_checksum_cache}) {
      $self->logconfess("Missing a populated MD5 cache file '$cache_file'",
                        "for '$path'");
    }
    else {
      $md5 = $self->irods->md5sum($path);

      if (-s $path > $self->checksum_cache_threshold) {
        $self->_make_md5_cache_file($cache_file, $md5);
      }
    }
  }

  return $md5;
}

sub _md5_cache_file_stale {
  my ($self, $path, $cache_file) = @_;

  my $path_stat  = stat $path;
  my $cache_stat = stat $cache_file;

  # Pipeline processes may write the data file and its checksum cache
  # in parallel, leading to mthe possibility that the checksum file
  # handle may be closed before the data file handle. i.e. the data
  # file may be newer than its checksum cache. The test for stale
  # cache files uses a delta to accommodate this; if the data file is
  # newer by more than delta seconds, the cache is considered stale.

  return (($path_stat->mtime - $cache_stat->mtime)
          > $self->checksum_cache_time_delta) ? 1 : 0;
}

sub _read_md5_cache_file {
  my ($self, $cache_file) = @_;

  my $md5 = q[];

  my $in;
  open $in, '<', $cache_file or
    $self->logcarp("Failed to open '$cache_file' for reading: $ERRNO");
  $md5 = <$in>;
  close $in or
    $self->logcarp("Failed to close '$cache_file' cleanly: $ERRNO");

  if ($md5) {
    chomp $md5;

    my $len = length $md5;
    if ($len != 32) {
      $self->error("Malformed ($len character) MD5 checksum ",
                   "'$md5' read from '$cache_file'");
    }
  }
  else {
    $self->warn("Malformed (empty) MD5 checksum read from '$cache_file'");
  }

  return $md5;
}

sub _make_md5_cache_file {
  my ($self, $cache_file, $md5) = @_;

  $self->debug("Adding missing MD5 cache file '$cache_file'");

  my ($filename, $cache_dir) = fileparse($cache_file);

  if (not -w $cache_dir) {
    $self->warn("Cache directory '$cache_dir' is not writable");
    return $cache_file;
  }
  if (not -x $cache_dir) {
    $self->warn("Cache directory '$cache_dir' is not executable");
    return $cache_file;
  }

  try {
    my $out;
    open $out, '>', $cache_file or
      croak "Failed to open '$cache_file' for writing: $ERRNO";
      print $out "$md5\n" or
        croak "Failed to write MD5 to '$cache_file': $ERRNO";
    close $out or
      $self->warn("Failed to close '$cache_file' cleanly: $ERRNO");
  } catch {
    # Failure to create a cache should not be a hard error. Here we
    # just forward the message from croak above.
    $self->error($_);
  };

  return $cache_file;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__

=head1 NAME

WTSI::NPG::iRODS::Publisher

=head1 DESCRIPTION

General purpose file/metadata publisher for iRODS. Objects of this
class provide the capability to:

 - Put new files into iRODS

 - Update (overwrite) files already in iRODS

 - Compare local (file system) checksums to remote (iRODS) checksums
   before an upload to determine whether work needs to be done.

 - Compare local (file system) checksums to remote (iRODS) checksums
   after an upload to determine that data were transferred successfully.

 - Cache local (file system) checksums for large files.

 - Add basic metadata to all uploaded files:

   - Creation timestamp

   - Update timestamp

   - File type

   - Entity performing the upload

   See WTSI::NPG::iRODS::Annotator.

 - Add custom metadata supplied by the caller.


=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2015, 2016, 2017, 2019, 2021 Genome Research Limited. All Rights
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
