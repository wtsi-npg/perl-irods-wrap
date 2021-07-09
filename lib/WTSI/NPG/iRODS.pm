package WTSI::NPG::iRODS;

use namespace::autoclean;
use version;
use Cache::LRU;
use DateTime;
use Data::Dump qw(pp);
use Encode qw(decode);
use English qw(-no_match_vars);
use File::Basename qw(basename fileparse);
use File::Spec::Functions qw(abs2rel canonpath catdir catfile splitdir);
use List::MoreUtils qw(any uniq);
use Log::Log4perl::Level;
use Moose;
use MooseX::StrictConstructor;
use Try::Tiny;

use WTSI::DNAP::Utilities::Runnable;

use WTSI::NPG::iRODS::Metadata qw($FILE_MD5 $STAGING);
use WTSI::NPG::iRODS::BatonClient;
use WTSI::NPG::iRODS::Types qw(:all);

our $VERSION = '';

our $MAX_BATON_VERSION = '2.99.99';
our $MIN_BATON_VERSION = '2.0.0';

our $IADMIN      = 'iadmin';
our $ICP         = 'icp';
our $IENV        = 'ienv';
our $IGET        = 'iget';
our $IGROUPADMIN = 'igroupadmin';
our $IMKDIR      = 'imkdir';
our $IMV         = 'imv';
our $IPUT        = 'iput';
our $IRM         = 'irm';

our $READ_PERMISSION  = 'read';
our $WRITE_PERMISSION = 'write';
our $OWN_PERMISSION   = 'own';
our $NULL_PERMISSION  = 'null';

our $PUBLIC_GROUP     = 'public';

our @VALID_PERMISSIONS = ($NULL_PERMISSION, $READ_PERMISSION,
                          $WRITE_PERMISSION, $OWN_PERMISSION);

our $DEFAULT_CACHE_SIZE = 128;
our $OBJECT_PATH        = 'OBJECT';
our $COLLECTION_PATH    = 'COLLECTION';

our $STAGING_RAND_MAX   = 1024 * 1024 * 1024;
our $STAGING_MAX_TRIES  = 2;

our $SKIP_CHECKSUM   = 0;
our $CALC_CHECKSUM   = 1;
our $VERIFY_CHECKSUM = 2;

has 'strict_baton_version' =>
  (is            => 'ro',
   isa           => 'Bool',
   required      => 1,
   default       => 1,
   documentation => 'Strictly check the baton version if true');

has 'environment' =>
  (is            => 'ro',
   isa           => 'HashRef',
   required      => 1,
   default       => sub { \%ENV },
   documentation => 'The shell environment in which iRODS clients are run');

has 'groups' =>
  (is             => 'ro',
   isa           => 'ArrayRef',
   required      => 1,
   lazy          => 1,
   builder       => '_build_groups',
   clearer       => 'clear_groups',
   init_arg      => undef,
   documentation => 'The iRODS data access groups, filtered by the ' .
                    'group_filter');

has 'group_prefix' =>
  (is            => 'rw',
   isa           => NoWhitespaceStr,
   required      => 1,
   default       => 'ss_',
   documentation => 'A prefix for group names used to distinguish ' .
                    'iRODS groups from users');

has 'group_filter' =>
  (is            => 'rw',
   isa           => 'Maybe[CodeRef]',
   required      => 1,
   lazy          => 1,
   default       => sub {
     my ($self) = @_;

     my $prefix = $self->group_prefix;

     return sub {
       my ($owner) = @_;

       if (defined $owner and $owner =~ m{^$prefix}msx) {
         return 1;
       }
     }
   },
   documentation => 'A filter predicate that returns true when passed an ' .
                    'iRODS owner that is a group name');

has 'working_collection' =>
  (is        => 'rw',
   isa       =>  AbsolutePath,
   predicate => 'has_working_collection',
   clearer   => 'clear_working_collection');

has 'baton_client' =>
  (is       => 'ro',
   isa      => 'WTSI::NPG::iRODS::BatonClient',
   required => 1,
   lazy     => 1,
   builder  => '_build_baton_client',
   predicate => 'has_baton_client');

has 'single_server' =>
  (is            => 'ro',
   isa           => 'Bool',
   default       => 0,
   documentation => 'If true, connect ony a single iRODS server by avoiding ' .
                    'any direct connections to resource servers. This mode will ' .
                    'be much slower to transfer large files, but does not ' .
                    'resource servers to be accessible');

has '_path_cache' =>
  (is            => 'ro',
   isa           => 'Cache::LRU',
   required      => 1,
   lazy          => 1,
   default       => sub { return Cache::LRU->new(size => $DEFAULT_CACHE_SIZE) },
   documentation => 'A cache mapping known iRODS paths to their type');

has '_metadata_cache' =>
  (is            => 'ro',
   isa           => 'Cache::LRU',
   required      => 1,
   lazy          => 1,
   default       => sub { return Cache::LRU->new(size => $DEFAULT_CACHE_SIZE) },
   documentation => 'A cache mapping known iRODS paths to their metadata');

has '_permissions_cache' =>
  (is            => 'ro',
   isa           => 'Cache::LRU',
   required      => 1,
   lazy          => 1,
   default       => sub { return Cache::LRU->new(size => $DEFAULT_CACHE_SIZE) },
   documentation => 'A cache mapping known iRODS paths to their permissions');


with 'WTSI::DNAP::Utilities::Loggable',
     'WTSI::NPG::iRODS::Utilities';

sub BUILD {
  my ($self) = @_;

  my $installed_baton_version = $self->installed_baton_version;

  if (not $self->match_baton_version($installed_baton_version)) {
    my $required_range = join q{ - }, $MIN_BATON_VERSION, $MAX_BATON_VERSION;
    my $msg = sprintf "The installed baton release version %s is " .
      "not supported by this wrapper (requires version %s)",
      $installed_baton_version, $required_range;

    if ($self->strict_baton_version) {
      $self->logdie($msg);
    }
    else {
      $self->warn($msg);
    }
  }

  return $self;
}

sub installed_baton_version {
  my ($self) = @_;

  my ($version) = WTSI::DNAP::Utilities::Runnable->new
    (executable  => 'baton-list',
     arguments   => ['--version'],
     environment => $self->environment)->run->split_stdout;

  return $version;
}

sub match_baton_version {
  my ($self, $version) = @_;

  defined $version or
    $self->logconfess('A defined version argument is required');

  my ($dotted_version, $commits) = $version =~ m{^(\d+[.]\d+[.]\d+)(\S*)$}msx;
  defined $dotted_version or
    $self->logconfess("Failed to baton parse version string '$version'");

  my $min = version->parse($MIN_BATON_VERSION);
  my $max = version->parse($MAX_BATON_VERSION);
  my $candidate = version->parse($dotted_version);

  my $match = ($candidate <= $max and $candidate >= $min);
  my $required_range = join q{ - }, uniq $MIN_BATON_VERSION, $MAX_BATON_VERSION;
  if ($match) {
    $self->debug("baton version $version matches $required_range");
  }
  else {
    $self->debug("baton version $version does not match $required_range");
  }

  return $match;
}

around 'working_collection' => sub {
  my ($orig, $self, @args) = @_;

  if (@args) {
    my $collection = $args[0];
    $collection eq q{} and
      $self->logconfess('A non-empty collection argument is required');

    $collection = $self->ensure_collection_path($collection);
    $self->debug("Changing working_collection to '$collection'");
    $self->$orig($collection);
  }
  elsif (!$self->has_working_collection) {
    $self->$orig($self->get_irods_home);
  }

  return $self->$orig;
};

=head2 absolute_path

  Arg [1]    : An iRODS path.

  Example    : $irods->absolute_path('./path')
  Description: Return an absolute iRODS path given a path.
  Returntype : Str

=cut

sub absolute_path {
  my ($self, $path) = @_;

  defined $path or $self->logconfess('A defined path argument is required');
  $path or $self->logconfess('A non-empty path argument is required');

  $path = canonpath($path);

  return $self->_ensure_absolute_path($path);
}

=head2 ensure_collection_path

  Arg [1]    : An iRODS path.

  Example    : $irods->ensure_collection_path('./path')
  Description: Given an iRODS collection, return its absolute path. Raise
               an error if the input path is not an iRODS collection.
  Returntype : Str

=cut

sub ensure_collection_path {
  my ($self, $target) = @_;

  if (not defined $target) {
      $self->logconfess('A defined collection argument is required');
  } elsif ($target eq q{}) {
      $self->logconfess('A non-empty collection argument is required');
  }
  my $path = $self->_ensure_absolute_path($target);
  if (not $self->is_collection($path)) {
    $self->logconfess("A collection path is required: received '$path'");
  }

  return $path;
}

=head2 ensure_object_path

  Arg [1]    : An iRODS path.

  Example    : $irods->ensure_object_path('./path')
  Description: Given an iRODS data object, return its absolute path. Raise
               an error if the input path is not an iRODS data object.
  Returntype : Str

=cut

sub ensure_object_path {
  my ($self, $target) = @_;

  if (not defined $target) {
      $self->logconfess('A defined object argument is required');
  } elsif ($target eq q{}) {
      $self->logconfess('A non-empty object argument is required');
  }
  my $path = $self->_ensure_absolute_path($target);
  if (not $self->is_object($path)) {
    $self->logconfess("A data object path is required: received '$path'");
  }

  return $path;
}

=head2 get_irods_env

  Arg [1]    : None.

  Example    : $irods->get_irods_env
  Description: Return the iRODS environment according to 'ienv'.
  Returntype : HashRef[Str]

=cut

sub get_irods_env {
  my ($self) = @_;

  my @entries = WTSI::DNAP::Utilities::Runnable->new
    (executable  => $IENV,
     environment => $self->environment)->run->split_stdout;

  shift @entries; # Discard version information line
  @entries or
    $self->logconfess("Failed to read any entries from '$IENV'");

  my %env;
  foreach my $entry (@entries) {
    my ($key, $value);
    next if $entry =~ m{is\snot\sdefined$}msx;
    ($key, $value) = $entry =~ m{^(?:NOTICE:\s+)?(\S+)\s-\s(.*)}msx;
    if (defined $key and defined $value) {
      $env{$key} = $value;
    }
    else {
      $self->warn("Failed to parse iRODS environment entry '$entry'");
    }
  }

  return \%env;
}

=head2 get_irods_user

  Arg [1]    : None.

  Example    : $irods->get_irods_user
  Description: Return an iRODS user name according to 'ienv'.
  Returntype : Str

=cut

sub get_irods_user {
  my ($self) = @_;

  my $user = $self->get_irods_env->{irods_user_name};
  defined $user or
    $self->logconfess("Failed to obtain the iRODS user name from '$IENV'");

  return $user;
}

=head2 get_irods_home

  Arg [1]    : None.

  Example    : $irods->get_irods_home
  Description: Return an iRODS user home collection according to 'ienv'.
  Returntype : Str

=cut

sub get_irods_home {
  my ($self) = @_;

  my $home = $self->get_irods_env->{irods_home};
  defined $home or
    $self->logconfess("Failed to obtain the iRODS home from '$IENV'");

  return $home;
}

=head2 find_zone_name

  Arg [1]    : An absolute iRODS path.

  Example    : $irods->find_zone('/zonename/path')
  Description: Return an iRODS zone name given a path.
  Returntype : Str

=cut

sub find_zone_name {
  my ($self, $path) = @_;

  defined $path or $self->logconfess('A defined path argument is required');

  $path = canonpath($path);
  my $abs_path = $self->_ensure_absolute_path($path);
  $abs_path =~ s/^\///msx;

  $self->debug("Determining zone from path '", $abs_path, q{'});

  # If no zone if given, assume the current zone
  unless ($abs_path) {
    $self->debug("Using '", $self->working_collection, "' to determine zone");
    $abs_path = $self->working_collection;
  }

  my @path = grep { $_ ne q{} } splitdir($abs_path);
  unless (@path) {
    $self->logconfess("Failed to parse iRODS zone from path '$path'");
  }

  my $zone = shift @path;
  return $zone;
}

=head2 make_group_name

  Arg [1]    : An identifier indicating group membership.

  Example    : $irods->make_group_name(1234)
  Description: Return an iRODS group name given an identifier e.g. a
               SequenceScape study ID.
  Returntype : Str

=cut

sub make_group_name {
  my ($self, $identifier) = @_;

  defined $identifier or
    $self->logconfess('A defined group identifier is required');
  $identifier eq q{} and
    $self->logconfess('A non-empty group identifier is required');
  is_NoWhitespaceStr($identifier) or
    $self->logconfess('A non-whitespace group identifier is required');

  return $self->group_prefix . $identifier;
}

=head2 list_groups

  Arg [1]    : None.

  Example    : $irods->list_groups
  Description: Returns a list of iRODS groups
  Returntype : Array

=cut

sub list_groups {
  my ($self) = @_;

  my @groups = WTSI::DNAP::Utilities::Runnable->new
    (executable  => $IGROUPADMIN,
     arguments   => ['lg'],
     environment => $self->environment)->run->split_stdout;

  return @groups;
}

=head2 group_exists

  Arg [1]    : iRODS group name
  Example    : group_exists($name)
  Description: Return true if the group exists, or false otherwise
  Returntype : Bool

=cut

sub group_exists {
  my ($self, $name) = @_;

  return any { $_ eq $name } @{$self->groups}; # Use the groups cache
}

=head2 add_group

  Arg [1]    : new iRODS group name.
  Example    : $irods->add_group($name)
  Description: Create a new group. Raises an error if the group exists
               already. Returns the group name. The group name is not escaped
               in any way.
  Returntype : Str

=cut

sub add_group {
  my ($self, $name) = @_;

  if ($self->group_exists($name)) {
    $self->logconfess("Failed to create iRODS group '$name' because it exists");
  }

  WTSI::DNAP::Utilities::Runnable->new(executable  => $IADMIN,
                                       arguments   => ['mkgroup', $name],
                                       environment => $self->environment)->run;
  $self->clear_groups; # Clear the groups cache

  return $name;
}

=head2 remove_group

  Arg [1]    : An existing iRODS group name.
  Example    : $irods->remove_group($name)
  Description: Remove a group. Raises an error if the group does not exist.
               already. Returns the group name. The group name is not escaped
               in any way.
  Returntype : Str

=cut

sub remove_group {
  my ($self, $name) = @_;

  unless ($self->group_exists($name)) {
    $self->logconfess("Unable to remove group '$name' because ",
                      "it doesn't exist");
  }

  WTSI::DNAP::Utilities::Runnable->new(executable  => $IADMIN,
                                       arguments   => ['rmgroup', $name],
                                       environment => $self->environment)->run;
  $self->clear_groups; # Clear the groups cache

  return $name;
}

=head2 set_group_access

  Arg [1]    : Permission, Str. One of $WTSI::NPG::iRODS::READ_PERMISSION,
               $WTSI::NPG::iRODS::WRITE_PERMISSION,
               $WTSI::NPG::iRODS::OWN_PERMISSION or
               $WTSI::NPG::iRODS::NULL_PERMISSION.
  Arg [2]    : An iRODS group name.  This may be of the form <group> or
               <group>#<zone>
  Arg [3]    : One or more data objects or collections.

  Example    : $irods->set_group_access($WTSI::NPG::iRODS::READ_PERMISSION,
                                        $WTSI::NPG::iRODS::PUBLIC_GROUP,
                                        $object1, $object2)
  Description: Set the access rights on one or more objects for a group,
               returning the objects.
  Returntype : Array

=cut

sub set_group_access {
  my ($self, $permission, $group, @objects) = @_;

  my $perm_str = defined $permission ? $permission : $NULL_PERMISSION;

  foreach my $object (@objects) {
    $self->set_object_permissions($perm_str, $group, $object);
  }

  return @objects;
}

=head2 reset_working_collection

  Arg [1]    : None.

  Example    : $irods->reset_working_collection
  Description: Reset the current iRODS working collection to the home
               collection and return self.
  Returntype : WTSI::NPG::iRODS

=cut

sub reset_working_collection {
  my ($self) = @_;

  $self->clear_working_collection;

  return $self;
}

=head2 is_collection

  Arg [1]    : Str iRODS path.

  Example    : $irods->is_collection('/path')
  Description: Return true if path is an iRODS collection.
  Returntype : Bool

=cut

sub is_collection {
  my ($self, $path) = @_;

  defined $path or
    $self->logconfess('A defined path argument is required');

  $path eq q{}
    and $self->logconfess('A non-empty path argument is required');

  $path = canonpath($path);
  $path = $self->_ensure_absolute_path($path);

  return $self->baton_client->is_collection($path);
}

=head2 list_collection

  Arg [1]    : Str iRODS collection path.
  Arg [2]    : Bool recurse flag.

  Example    : my ($objs, $colls) = $irods->list_collection($coll)
  Description: Return the contents of the collection as two arrayrefs,
               the first listing data objects, the second listing nested
               collections.
  Returntype : Array

=cut

sub list_collection {
  my ($self, $collection, $recurse) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection eq q{}
    and $self->logconfess('A non-empty collection argument is required');

  $collection = canonpath($collection);
  $collection = $self->_ensure_absolute_path($collection);

  # TODO: We could check that the collection exists here. However,
  # current behaviour is to return undef rather than raise an
  # exception.

  my $recursively = $recurse ? 'recursively' : q{};
  $self->debug("Listing collection '$collection' $recursively");

  return $self->baton_client->list_collection($collection, $recurse);
}

=head2 add_collection

  Arg [1]    : iRODS collection path.

  Example    : $irods->add_collection('/my/path/foo')
  Description: Make a new collection in iRODS. Return the new collection.
  Returntype: Str
=cut

sub add_collection {
  my ($self, $collection) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection eq q{}
    and $self->logconfess('A non-empty collection argument is required');

  $collection = canonpath($collection);
  $collection = $self->_ensure_absolute_path($collection);
  $self->debug("Adding collection '$collection'");

  WTSI::DNAP::Utilities::Runnable->new(executable  => $IMKDIR,
                                       arguments   => ['-p', $collection],
                                       environment => $self->environment)->run;
  return $collection;
}

=head2 put_collection

  Arg [1]    : Local directory path.
  Arg [2]    : iRODS collection path.

  Example    : $irods->put_collection('/my/path/foo', '/archive')
  Description: Make a new collection in iRODS. Return the new collection.
  Returntype : Str

=cut

sub put_collection {
  my ($self, $dir, $target) = @_;

  defined $dir or
    $self->logconfess('A defined directory argument is required');
  defined $target or
    $self->logconfess('A defined target (collection) argument is required');

  $dir eq q{} and
    $self->logconfess('A non-empty directory argument is required');
  $target eq q{} and
    $self->logconfess('A non-empty target (collection) argument is required');

  # iput does not accept trailing slashes on directories
  $dir = canonpath($dir);
  $target = $self->ensure_collection_path($target);
  $self->debug("Putting directory '$dir' into collection '$target'");

  my @args = ('-k', '-r', $dir, $target);
  WTSI::DNAP::Utilities::Runnable->new(executable  => $IPUT,
                                       arguments   => \@args,
                                       environment => $self->environment)->run;

  return $target . q{/} . basename($dir);
}

=head2 move_collection

  Arg [1]    : iRODS collection path, which must exist.
  Arg [2]    : iRODS collection path.

  Example    : $irods->move_collection('/my/path/a', '/my/path/b')
  Description: Move a collection.
  Returntype : Str

=cut

sub move_collection {
  my ($self, $source, $target) = @_;

  defined $source or
    $self->logconfess('A defined source (collection) argument is required');
  defined $target or
    $self->logconfess('A defined target (collection) argument is required');

  $source eq q{} and
    $self->logconfess('A non-empty source (collection) argument is required');
  $target eq q{} and
    $self->logconfess('A non-empty target (collection) argument is required');

  $source = $self->ensure_collection_path($source);
  $target = canonpath($target);
  $target = $self->_ensure_absolute_path($target);
  $self->debug("Moving collection from '$source' to '$target'");

  # Due to a bug in iRODS 4.2.7, imv doesn't work cleanly for collections.
  # This is especially evident on federated zones. This is a workaround that
  # serialises the operation.

  my ($source_objs, $source_colls) =
    $self->list_collection($source, 'RECURSE');

  # Handle collections
  foreach my $source_coll (@{$source_colls}) {
    my $rel = abs2rel($source_coll, $source);
    my $target_coll = catdir($target, $rel);

    $self->debug("Creating target collection $target_coll ",
                 "for $source_coll");
    $self->add_collection($target_coll);

    foreach my $avu ($self->get_collection_meta($source_coll)) {
      my ($attribute, $value, $units) = ($avu->{attribute},
                                         $avu->{value},
                                         $avu->{units});
      my $units_str = defined $units ? "'$units'" : "'undef'";

      $self->debug("Copying AVU ['$attribute', '$value', $units_str] ",
                   "from '$source_coll' to '$target_coll'");
      $self->add_collection_avu($target_coll, $attribute, $value, $units);
    }
  }

  # Handle data objects
  foreach my $source_obj (@{$source_objs}) {
    my $rel = abs2rel($source_obj, $source);
    my $target_obj = catfile($target, $rel);

    WTSI::DNAP::Utilities::Runnable      ->new
      (executable  => $IMV,
       arguments   => [ $source_obj, $target_obj ],
       environment => $self->environment)->run;
  }

  # Clean up source collections safely
  $self->remove_collection_safely($source);

  return $target;
}

=head2 get_collection

  Arg [1]    : iRODS collection path.
  Arg [2]    : Local directory path.

  Example    : $irods->get_collection('/my/path/foo', '.')
  Description: Fetch a collection and contents, recursively and return
               the path of the local copy.
  Returntype : Str

=cut

sub get_collection {
  my ($self, $source, $target) = @_;

  defined $source or
    $self->logconfess('A defined source (collection) argument is required');
  defined $target or
    $self->logconfess('A defined target (directory) argument is required');

  $source eq q{} and
    $self->logconfess('A non-empty source (collection) argument is required');
  $target eq q{} and
    $self->logconfess('A non-empty target (directory) argument is required');

  $source = $self->ensure_collection_path($source);
  $target = canonpath($target);
  $self->debug("Getting from '$source' to '$target'");

  my @args = ('-r', '-f', $source, $target);
  WTSI::DNAP::Utilities::Runnable->new(executable  => $IGET,
                                       arguments   => \@args,
                                       environment => $self->environment)->run;
  return $self;
}

=head2 remove_collection

  Arg [1]    : iRODS collection path.

  Example    : $irods->remove_collection('/my/path/foo')
  Description: Remove a collection and contents, recursively, and return
               self.
  Returntype : WTSI::NPG::iRODS

=cut

sub remove_collection {
  my ($self, $collection) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection eq q{} and
    $self->logconfess('A non-empty collection argument is required');

  $collection = $self->ensure_collection_path($collection);
  $self->debug("Removing collection '$collection'");

  WTSI::DNAP::Utilities::Runnable->new(executable  => $IRM,
                                       arguments   => ['-r', '-f', $collection],
                                       environment => $self->environment)->run;
  return $collection;
}

=head2 remove_collection_safely

  Arg [1]    : iRODS collection path.

  Example    : $irods->remove_collection_safely('/my/path/foo')
  Description: Remove a collection and contents, recursively, and return
               self. Contents are only removed if they are empty collections.
  Returntype : WTSI::NPG::iRODS

=cut

sub remove_collection_safely {
  my ($self, $collection) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection eq q{} and
    $self->logconfess('A non-empty collection argument is required');

  $collection = $self->ensure_collection_path($collection);
  $self->debug("Removing collection '$collection'");

  $self->baton_client->remove_collection_safely($collection, 'RECURSE');
  return $collection;
}

=head2 get_collection_permissions

  Arg [1]    : iRODS collection path.

  Example    : $irods->get_collection_permissions($path)
  Description: Return a list of ACLs defined for a collection.
  Returntype : Array

=cut

sub get_collection_permissions {
  my ($self, $collection) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection eq q{}
    and $self->logconfess('A non-empty collection argument is required');

  $collection = $self->ensure_collection_path($collection);

  return $self->sort_acl($self->baton_client->get_collection_acl($collection));
}

=head2 set_collection_permissions

  Arg [1]    : Permission, Str. One of $WTSI::NPG::iRODS::READ_PERMISSION,
               $WTSI::NPG::iRODS::WRITE_PERMISSION,
               $WTSI::NPG::iRODS::OWN_PERMISSION or
               $WTSI::NPG::iRODS::NULL_PERMISSION.
  Arg [2]    : Owner (user or group). This may be of the form <user> or
               <user>#<zone>.

  Example    : $irods->set_collection_permissions('read', 'user1', $path)
  Description: Set access permissions on the collection. Return the collection
               path.
  Returntype : Str

=cut


sub set_collection_permissions {
  my ($self, $level, $owner, $collection) = @_;

  defined $owner or
    $self->logconfess('A defined owner argument is required');
  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $owner eq q{} and
    $self->logconfess('A non-empty owner argument is required');
  $collection eq q{} and
    $self->logconfess('A non-empty collection argument is required');

  $collection = $self->ensure_collection_path($collection);

  my $perm_str = defined $level ? $level : 'null';

  any { $perm_str eq $_ } @VALID_PERMISSIONS or
    $self->logconfess("Invalid permission level '$perm_str'");

  $self->debug("Setting permissions on '$collection' to ",
               "'$perm_str' for '$owner'");

  my @acl = $self->get_collection_permissions($collection);

  my ($owner_name, $zone) = split /\#/msx, $owner;
  $zone ||= $self->find_zone_name($collection);

  if (any { $_->{owner} eq $owner_name and
            $_->{zone}  eq $zone       and
            $_->{level} eq $perm_str } @acl) {
    $self->debug("'$collection' already has permission ",
                 "'$perm_str' for '$owner_name#$zone'");
  }
  else {
    $self->baton_client->chmod_collection($perm_str, $owner, $collection);
  }

  return $collection;
}

=head2 get_collection_groups

  Arg [1]    : iRODS collection path.
  Arg [2]    : Permission, Str.  One of $WTSI::NPG::iRODS::READ_PERMISSION,
               $WTSI::NPG::iRODS::WRITE_PERMISSION,
               $WTSI::NPG::iRODS::OWN_PERMISSION or
               $WTSI::NPG::iRODS::NULL_PERMISSION. Optional.

  Example    : $irods->get_collection_groups($path)
  Description: Return a list of the data access groups in the collection's ACL.
               If a permission level argument is supplied, only groups with
               that level of access will be returned. Only groups having a
               group name matching the current group filter will be returned.
  Returntype : Array

=cut

sub get_collection_groups {
  my ($self, $collection, $level) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');
  $collection eq q{} and
    $self->logconfess('A non-empty collection argument is required');

  $collection = $self->ensure_collection_path($collection);

  my $perm_str = defined $level ? $level : $NULL_PERMISSION;

  any { $perm_str eq $_ } @VALID_PERMISSIONS or
    $self->logconfess("Invalid permission level '$perm_str'");

  my @perms = $self->get_collection_permissions($collection);
  if ($level) {
    @perms = grep { $_->{level} eq $perm_str } @perms;
  }

  my @owners = map { $_->{owner} } @perms;
  if ($self->group_filter) {
    $self->debug("Pre-filter owners of '$collection': [",
                 join(q{, }, @owners), q{]});
    @owners = grep { $self->group_filter->($_) } @owners;
    $self->debug("Post-filter owners of '$collection': [",
                 join(q{, }, @owners), q{]});
  }

  my @groups = sort @owners;

  return @groups;
}

=head2 get_collection_meta

  Arg [1]    : iRODS data collection path.

  Example    : $irods->get_collection_meta('/my/path/')
  Description: Get metadata on a collection as an array of AVUs.
  Returntype : Array[HashRef]

=cut

sub get_collection_meta {
  my ($self, $collection) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection eq q{} and
    $self->logconfess('A non-empty collection argument is required');

  $collection = $self->ensure_collection_path($collection);

  my @avus = $self->baton_client->list_collection_meta($collection);

  return $self->sort_avus(@avus);
}

=head2 add_collection_avu

  Arg [1]    : iRODS collection path.
  Arg [2]    : attribute.
  Arg [3]    : value.
  Arg [4]    : units (optional).

  Example    : $irods->add_collection_avu('/my/path/foo', 'id', 'ABCD1234')
  Description: Add metadata to a collection. Return an array of
               the new attribute, value and units.
  Returntype : Array

=cut

sub add_collection_avu {
  my ($self, $collection, $attribute, $value, $units) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');
  defined $attribute or
    $self->logconfess('A defined attribute argument is required');
  defined $value or
    $self->logconfess('A defined value argument is required');

  $collection eq q{} and
    $self->logconfess('A non-empty collection argument is required');
  $attribute eq q{} and
    $self->logconfess('A non-empty attribute argument is required');
  $value eq q{} and
    $self->logconfess('A non-empty value argument is required');

  my $units_str = defined $units ? "'$units'" : "'undef'";

  $collection = $self->ensure_collection_path($collection);
  $self->debug("Adding AVU ['$attribute', '$value', $units_str] ",
               "to '$collection'");

  my @current_meta = $self->get_collection_meta($collection);
  if ($self->_meta_exists($attribute, $value, $units, \@current_meta)) {
    $self->logconfess("AVU ['$attribute', '$value', $units_str] ",
                      "already exists for '$collection'");
  }

  return $self->baton_client->add_collection_avu($collection, $attribute,
                                                 $value, $units);
}

=head2 remove_collection_avu

  Arg [1]    : iRODS collection path.
  Arg [2]    : attribute.
  Arg [3]    : value.
  Arg [4]    : units (optional).

  Example    : $irods->remove_collection_avu('/my/path/foo', 'id', 'ABCD1234')
  Description: Removes metadata from a collection object. Return the
               collection path.
  Returntype : Str

=cut

sub remove_collection_avu {
  my ($self, $collection, $attribute, $value, $units) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');
  defined $attribute or
    $self->logconfess('A defined attribute argument is required');
  defined $value or
    $self->logconfess('A defined value argument is required');

  $collection eq q{} and
    $self->logconfess('A non-empty collection argument is required');
  $attribute eq q{} and
    $self->logconfess('A non-empty attribute argument is required');
  $value eq q{} and
    $self->logconfess('A non-empty value argument is required');

  my $units_str = defined $units ? "'$units'" : "'undef'";

  $collection = $self->ensure_collection_path($collection);
  $self->debug("Removing AVU ['$attribute', '$value', $units_str] ",
               "from '$collection'");

  my @current_meta = $self->get_collection_meta($collection);
  if (!$self->_meta_exists($attribute, $value, $units, \@current_meta)) {
    $self->logcluck("AVU ['$attribute', '$value', $units_str] ",
                    "does not exist for '$collection'");
  }

  return $self->baton_client->remove_collection_avu($collection, $attribute,
                                                    $value, $units);
}

=head2 make_collection_avu_history

  Arg [1]    : iRODS collection path.
  Arg [2]    : attribute.
  Arg [3]    : DateTime a timestamp (optional, defaults to the current time).

  Example    : $irods->make_collection_avu_history('/my/path/lorem.txt', 'id');
  Description: Return a new history AVU reflecting the current state of
               the attribute. i.e. call this method before you change the
               AVU.

               The history will be of the form:

               [<ISO8601 timestamp>] <value>[,<value>]+

               If there are multiple AVUS for the specified attribute, their
               values will be sorted and concatenated, separated by commas.
               If there are no AVUs specified attribute, an error will be
               raised.
  Returntype : HashRef

=cut

sub make_collection_avu_history {
  my ($self, $collection, $attribute, $timestamp) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');
  defined $attribute or
    $self->logconfess('A defined attribute argument is required');

  $collection eq q{} and
    $self->logconfess('A non-empty collection argument is required');
  $attribute eq q{} and
    $self->logconfess('A non-empty attribute argument is required');

  $collection = $self->ensure_collection_path($collection);

  my @historic_avus = grep { $_->{attribute} eq $attribute }
    $self->get_collection_meta($collection);
  unless (@historic_avus) {
    $self->logconfess("Failed to make a history for attribute '$attribute' ",
                      "on collection '$collection' because there are no AVUs ",
                      "with that attribute");
  }

  return $self->_make_avu_history($attribute, \@historic_avus, $timestamp);
}

=head2 find_collections_by_meta

  Arg [1]    : iRODS collection path.
  Arg [2]    : ArrayRef attribute value tuples

  Example    : $irods->find_collections_by_meta('/my/path/foo/',
                                                ['id' => 'ABCD1234'])
  Description: Find collections by their metadata, restricted to a parent
               collection. The collection path argument is not a simple
               string prefix, it is a collection. i.e. '/my/path/foo' is
               equivalent to '/my/path/foo/' and will not return results
               in collection '/my/path/foo_1'.
               Return a list of collections, sorted by their path.
  Returntype : Array

=cut

sub find_collections_by_meta {
  my ($self, $root, @query_specs) = @_;

  defined $root or $self->logconfess('A defined root argument is required');
  $root eq q{} and $self->logconfess('A non-empty root argument is required');

  $root = $self->ensure_collection_path($root);

  # Ensure a single trailing slash for collection boundary matching.
  $root =~ s/\/*$/\//msx;

  my $zone = $self->find_zone_name($root);
  # baton >= 0.10.0 uses paths as per-query zone hints
  my $zone_path = "/$zone";

  my @avu_specs;
  foreach my $query_spec (@query_specs) {
    my ($attribute, $value, $operator) = @$query_spec;

    my $spec = {attribute => $attribute,
                value     => $value};
    if ($operator) {
      $spec->{operator} = $operator;
    }

    push @avu_specs, $spec;
  }

  my $results = $self->baton_client->search_collections($zone_path,
                                                        @avu_specs);
  $self->debug("Found ", scalar @$results,
               "collections (to filter by '$root')");

  my @sorted = sort { $a cmp $b } @$results;
  $self->debug("Sorted ", scalar @sorted,
               " collections (to filter by '$root')");

  return grep { /^$root/msx } @sorted;
}

=head2 is_object

  Arg [1]    : Str iRODS path.

  Example    : $irods->is_object('/path')
  Description: Return true if path is an iRODS data object.
  Returntype : Bool

=cut

sub is_object {
  my ($self, $path) = @_;

  defined $path or
    $self->logconfess('A defined path argument is required');

  $path eq q{}
    and $self->logconfess('A non-empty path argument is required');

  $path = canonpath($path);
  $path = $self->_ensure_absolute_path($path);

  my $is_object = 0;
  my $cached = $self->_path_cache->get($path);
  if (defined $cached and $cached eq $OBJECT_PATH) {
    $self->debug("Using cached is_object for '$path'");
    $is_object = 1;
  }
  else {
    $is_object = $self->baton_client->is_object($path);
    if ($is_object) {
      $self->debug("Caching is_object for '$path'");
      $self->_path_cache->set($path, $OBJECT_PATH);
    }
  }

  return $is_object;
}

=head2 list_object

  Arg [1]    : iRODS data object path.

  Example    : $obj = $irods->list_object($object)
  Description: Return the full path of the object.
  Returntype : Str

=cut

sub list_object {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object eq q{} and
    $self->logconfess('A non-empty object argument is required');

  $object = $self->_ensure_absolute_path($object);

  # TODO: We could check that the object exists here. However, current
  # behaviour is to return undef rather than raise an exception.

  $self->debug("Listing object '$object'");

  my $result;
  if ($self->is_object($object)) {
    $result = $object; # Optimisation to use the path_cache
  }
  else {
    $result = $self->baton_client->list_object($object);
  }

  return $result;
}

=head2 read_object

  Arg [1]    : iRODS data object path.

  Example    : $irods->read_object('/my/path/lorem.txt')
  Description: Read a data object's contents into a string.
  Returntype : Str

=cut

sub read_object {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object eq q{} and
    $self->logconfess('A non-empty object argument is required');

  $object = $self->ensure_object_path($object);
  $self->debug("Reading object '$object'");

  return $self->baton_client->read_object($object);
}

=head2 add_object

  Arg [1]    : Path of file to add to iRODs.
  Arg [2]    : iRODS data object path.
  Arg [3]    : Checksum action, one of

               $WTSI::NPG::iRODS::CALC_CHECKSUM (calculate a checksum on the
               server side)

               $WTSI::NPG::iRODS::VERIFY_CHECKSUM (calculate a checksum on
               the server side and validate it against a checksum calculated
               on the client side)

               $WTSI::NPG::iRODS::SKIP_CHECKSUM (skip calculation of a
               checksum on the server side)

               Defaults to $WTSI::NPG::iRODS::SKIP_CHECKSUM.

  Example    : $irods->add_object('lorem.txt', '/my/path/lorem.txt')
  Description: Add a file to iRODS.
  Returntype : Str

=cut

sub add_object {
  my ($self, $file, $target, $checksum_action) = @_;

  defined $file or
    $self->logconfess('A defined file argument is required');
  defined $target or
    $self->logconfess('A defined target (object) argument is required');

  $file eq q{} and
    $self->logconfess('A non-empty file argument is required');
  $target eq q{} and
    $self->logconfess('A non-empty target (object) argument is required');

  if (defined $checksum_action) {
    ($checksum_action =~ m{^\d$}msx and
     any { $checksum_action == $_ } ($SKIP_CHECKSUM,
                                     $CALC_CHECKSUM,
                                     $VERIFY_CHECKSUM)) or
      $self->logconfess("Invalid checksum action '$checksum_action'");
  }
  else {
    $checksum_action = $SKIP_CHECKSUM;
  }

  $target = $self->_ensure_absolute_path($target);

  # Account for the target being a collection
  if ($self->is_collection($target)) {
    my ($file_name, $directories, $suffix) = fileparse($file);
    $target = catfile($target, $file_name);
  }

  $self->debug("Adding '$file' as new object '$target'");

  return $self->baton_client->put_object($file, $target, $checksum_action);
}

=head2 replace_object

  Arg [1]    : Path of file to add to iRODs.
  Arg [2]    : iRODS data object path.
  Arg [3]    : Checksum action, one of

               $WTSI::NPG::iRODS::CALC_CHECKSUM (calculate a checksum on the
               server side)

               $WTSI::NPG::iRODS::VERIFY_CHECKSUM (calculate a checksum on
               the server side and validate it against a checksum calculated
               on the client side)

               $WTSI::NPG::iRODS::SKIP_CHECKSUM (skip calculation of a
               checksum on the server side)

               Defaults to $WTSI::NPG::iRODS::SKIP_CHECKSUM.

  Example    : $irods->replace_object('lorem.txt', '/my/path/lorem.txt')
  Description: Replace a file in iRODS.
  Returntype : Str

=cut

sub replace_object {
  my ($self, $file, $target, $checksum_action) = @_;

  defined $file or
    $self->logconfess('A defined file argument is required');
  defined $target or
    $self->logconfess('A defined target (object) argument is required');

  $file eq q{} and
    $self->logconfess('A non-empty file argument is required');
  $target eq q{} and
    $self->logconfess('A non-empty target (object) argument is required');

  if (defined $checksum_action) {
    ($checksum_action =~ m{^\d$}msx and
     any { $checksum_action == $_ } ($SKIP_CHECKSUM,
                                     $CALC_CHECKSUM,
                                     $VERIFY_CHECKSUM)) or
      $self->logconfess("Invalid checksum action '$checksum_action'");
  }
  else {
    $checksum_action = $SKIP_CHECKSUM;
  }

  $target = $self->ensure_object_path($target);
  $self->debug("Replacing object '$target' with '$file'");

  return $self->baton_client->put_object($file, $target, $checksum_action);
}

=head2 copy_object

  Arg [1]    : iRODS data object path.
  Arg [2]    : iRODS data object path.
  Arg [3]    : iRODS metadata attribute translator (optional).

  Example    : $irods->copy_object('/my/path/lorem.txt', '/my/path/ipsum.txt',
                                   sub { 'copy_' . $_ })
  Description: Copy a data object, including all of its metadata. The
               optional third argument is a callback that may be used to
               translate metadata attributes during the copy.
  Returntype : Str

=cut

sub copy_object {
  my ($self, $source, $target, $translator) = @_;

  defined $source or
    $self->logconfess('A defined source (object) argument is required');
  defined $target or
    $self->logconfess('A defined target (object) argument is required');

  $source eq q{} and
    $self->logconfess('A non-empty source (object) argument is required');
  $target eq q{} and
    $self->logconfess('A non-empty target (object) argument is required');

  if (defined $translator) {
    ref $translator eq 'CODE' or
      $self->logconfess("translator argument must be a CodeRef");
  }

  $source = $self->ensure_object_path($source);
  $target = $self->_ensure_absolute_path($target);

  if ($self->is_collection($target)) {
    $self->logconfess("A target (object) argument may not be a collection: ",
                      "received '$target'");
  }

  $self->debug("Copying object from '$source' to '$target'");

  WTSI::DNAP::Utilities::Runnable->new(executable  => $ICP,
                                       arguments   => [$source, $target],
                                       environment => $self->environment)->run;

  $self->debug("Copying metadata from '$source' to '$target'");

  my @source_meta = $self->get_object_meta($source);
  foreach my $avu (@source_meta) {
    my $attr = $avu->{attribute};
    if ($translator) {
      $attr = $translator->($attr);
    }

    $self->add_object_avu($target, $attr, $avu->{value}, $avu->{units});
  }

  return $target
}

=head2 move_object

  Arg [1]    : iRODS data object path.
  Arg [2]    : iRODS data object path.

  Example    : $irods->move_object('/my/path/lorem.txt', '/my/path/ipsum.txt')
  Description: Move a data object.
  Returntype : Str

=cut

sub move_object {
  my ($self, $source, $target) = @_;

  defined $source or
    $self->logconfess('A defined source (object) argument is required');
  defined $target or
    $self->logconfess('A defined target (object) argument is required');

  $source eq q{} and
    $self->logconfess('A non-empty source (object) argument is required');
  $target eq q{} and
    $self->logconfess('A non-empty target (object) argument is required');

  $source = $self->ensure_object_path($source);
  $target = $self->_ensure_absolute_path($target);
  $self->debug("Moving object from '$source' to '$target'");
  $self->_clear_caches($source);

  WTSI::DNAP::Utilities::Runnable->new(executable  => $IMV,
                                       arguments   => [$source, $target],
                                       environment => $self->environment)->run;
  return $target
}

=head2 get_object

  Arg [1]    : iRODS data object path.
  Arg [2]    : Local file path.

  Example    : $irods->get_object('/my/path/lorem.txt', 'lorem.txt')
  Description: Fetch a data object and return the path of the local copy.
  Returntype : Str

=cut

sub get_object {
  my ($self, $source, $target) = @_;

  defined $source or
    $self->logconfess('A defined source (data object) argument is required');
  defined $target or
    $self->logconfess('A defined target (file) argument is required');

  $source eq q{} and
    $self->logconfess('A non-empty source (data object) argument is required');
  $target eq q{} and
    $self->logconfess('A non-empty target (file) argument is required');

  $source = $self->ensure_object_path($source);
  $target = $self->_ensure_absolute_path($target);

  my @args = ('-f', '-T', $source, $target);
  my $runnable = WTSI::DNAP::Utilities::Runnable->new
    (executable  => $IGET,
     arguments   => \@args)->run;

  return $target;
}

=head2 remove_object

  Arg [1]    : iRODS data object path.

  Example    : $irods->remove_object('/my/path/lorem.txt')
  Description: Remove a data object.
  Returntype : Str

=cut

sub remove_object {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object eq q{} and
    $self->logconfess('A non-empty object argument is required');

  $object = $self->ensure_object_path($object);
  $self->debug("Removing object '$object'");
  $self->_clear_caches($object);

  WTSI::DNAP::Utilities::Runnable->new(executable  => $IRM,
                                       arguments   => [$object],
                                       environment => $self->environment)->run;
  return $object;
}

=head2 slurp_object

  Arg [1]    : iRODS data object path.

  Example    : $irods->read_object('/my/path/lorem.txt')
  Description: Read a data object's contents into a string. (Synonym for
               read_object.)
  Returntype : Str

=cut

sub slurp_object {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object eq q{} and
    $self->logconfess('A non-empty object argument is required');

  $object = $self->ensure_object_path($object);
  $self->debug("Slurping object '$object'");

  return $self->read_object($object);
}

=head2 get_object_permissions

  Arg [1]    : iRODS data object path.

  Example    : $irods->get_object_permissions($path)
  Description: Return a list of ACLs defined for an object.
  Returntype : Array

=cut

sub get_object_permissions {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object eq q{} and
    $self->logconfess('A non-empty object argument is required');

  $object = $self->ensure_object_path($object);

  my $cached = $self->_permissions_cache->get($object);
  if (defined $cached) {
    $self->debug("Using cached ACL for '$object': ", pp($cached));
  }
  else {
    my @acl = $self->baton_client->get_object_acl($object);
    $cached = $self->_cache_permissions($object, \@acl);
  }

  return @{$cached};
}

=head2 set_object_permissions

  Arg [1]    : Permission, Str. One of $WTSI::NPG::iRODS::READ_PERMISSION,
               $WTSI::NPG::iRODS::WRITE_PERMISSION,
               $WTSI::NPG::iRODS::OWN_PERMISSION or
               $WTSI::NPG::iRODS::NULL_PERMISSION.
  Arg [2]    : Owner (user or group). This may be of the form <user> or
               <user>#<zone>.
  Arg [3]    : Path, Str.

  Example    : $irods->set_object_permissions('read', 'user1', $path)
  Description: Set access permissions on the data objecrt. Return the object
               path.
  Returntype : Str

=cut

sub set_object_permissions {
  my ($self, $level, $owner, $object) = @_;

  defined $owner or
    $self->logconfess('A defined owner argument is required');
  defined $object or
    $self->logconfess('A defined object argument is required');

  $owner eq q{} and
    $self->logconfess('A non-empty owner argument is required');
  $object eq q{} and
    $self->logconfess('A non-empty object argument is required');

  $object = $self->ensure_object_path($object);

  my $perm_str = defined $level ? $level : $NULL_PERMISSION;

  any { $perm_str eq $_ } @VALID_PERMISSIONS or
    $self->logconfess("Invalid permission level '$perm_str'");

  $self->debug("Setting permissions on '$object' to '$perm_str' for '$owner'");
  my @acl = $self->get_object_permissions($object);

  my ($owner_name, $zone) = split /\#/msx, $owner;
  $zone ||= $self->find_zone_name($object);

  if (any { $_->{owner} eq $owner_name and
            $_->{zone}  eq $zone       and
            $_->{level} eq $perm_str } @acl) {
    $self->debug("'$object' already has permission ",
                 "'$perm_str' for '$owner_name#$zone'");
  }
  else {
    $self->baton_client->chmod_object($perm_str, $owner, $object);

    # Having 'null' permission means having no permission, so these
    # must be removed from the cached ACL.
    my @remain = grep { not ($_->{owner} eq $owner_name and
                             $_->{zone}  eq $zone) } @acl;

    my $cached = $self->_cache_permissions($object,
                                           [@remain, {owner => $owner_name,
                                                      zone  => $zone,
                                                      level => $perm_str}]);
  }

  return $object;
}

=head2 get_object_groups

  Arg [1]    : iRODS data object path.
  Arg [2]    : Permission, Str. One of $WTSI::NPG::iRODS::READ_PERMISSION,
               $WTSI::NPG::iRODS::WRITE_PERMISSION,
               $WTSI::NPG::iRODS::OWN_PERMISSION or
               $WTSI::NPG::iRODS::NULL_PERMISSION. Optional.

  Example    : $irods->get_object_groups($path)
  Description: Return a list of the data access groups in the object's ACL.
               If a permission level argument is supplied, only groups with
               that level of access will be returned. Only groups having a
               group name matching the current group filter will be returned.
  Returntype : Array

=cut

sub get_object_groups {
  my ($self, $object, $level) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');
  $object eq q{} and
    $self->logconfess('A non-empty object argument is required');

  $object = $self->ensure_object_path($object);

  my $perm_str = defined $level ? $level : $NULL_PERMISSION;

  any { $perm_str eq $_ } @VALID_PERMISSIONS or
    $self->logconfess("Invalid permission level '$perm_str'");

  my @perms = $self->get_object_permissions($object);
  if ($level) {
    @perms = grep { $_->{level} eq $perm_str } @perms;
  }

  my @owners = map { $_->{owner} } @perms;
  if ($self->group_filter) {
    $self->debug("Pre-filter owners of '$object': [",
                 join(q{, }, @owners), q{]});
    @owners = grep { $self->group_filter->($_) } @owners;
    $self->debug("Post-filter owners of '$object': [",
                 join(q{, }, @owners), q{]});
  }

  my @groups = sort @owners;

  return @groups;
}

=head2 get_object_meta

  Arg [1]    : iRODS data object path.

  Example    : $irods->get_object_meta('/my/path/lorem.txt')
  Description: Get metadata on a data object as an array of AVUs.
  Returntype : Array[HashRef]

=cut

sub get_object_meta {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');
  $object eq q{} and
    $self->logconfess('A non-empty object argument is required');

  $object = $self->ensure_object_path($object);

  my $cached = $self->_metadata_cache->get($object);
  if (defined $cached) {
    $self->debug("Using cached AVUs for '$object': ", pp($cached));
  }
  else {
    my @avus = $self->baton_client->list_object_meta($object);
    $cached = $self->_cache_metadata($object, \@avus);
  }

  return @{$cached};
}

=head2 add_object_avu

  Arg [1]    : iRODS data object path.
  Arg [2]    : attribute.
  Arg [3]    : value.
  Arg [4]    : units (optional).

  Example    : add_object_avu('/my/path/lorem.txt', 'id', 'ABCD1234')
  Description: Add metadata to a data object. Return the object path.
  Returntype : Str

=cut

sub add_object_avu {
  my ($self, $object, $attribute, $value, $units) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');
  defined $attribute or
    $self->logconfess('A defined attribute argument is required');
  defined $value or
    $self->logconfess('A defined value argument is required');

  $object eq q{} and
    $self->logconfess('A non-empty object argument is required');
  $attribute eq q{} and
    $self->logconfess('A non-empty attribute argument is required');
  $value eq q{} and
    $self->logconfess('A non-empty value argument is required');

  $object = $self->ensure_object_path($object);

  my $avu = $self->make_avu($attribute, $value, $units);
  my $avu_str = $self->avu_str($avu);
  $self->debug("Adding AVU $avu_str to '$object'");

  my @current_meta = $self->get_object_meta($object);
  if ($self->_meta_exists($attribute, $value, $units, \@current_meta)) {
    $self->logconfess("AVU $avu_str already exists for '$object'");
  }
  else {
    $self->baton_client->add_object_avu($object, $attribute, $value,
                                        $units);
    $self->_cache_metadata($object, [@current_meta, $avu]);
  }

  return $object;
}

=head2 remove_object_avu

  Arg [1]    : iRODS data object path.
  Arg [2]    : attribute.
  Arg [3]    : value.
  Arg [4]    : units (optional).

  Example    : $irods->remove_object_avu('/my/path/lorem.txt', 'id',
               'ABCD1234')
  Description: Remove metadata from a data object. Return the object
               path.
  Returntype : Str

=cut

sub remove_object_avu {
  my ($self, $object, $attribute, $value, $units) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');
  defined $attribute or
    $self->logconfess('A defined attribute argument is required');
  defined $value or
    $self->logconfess('A defined value argument is required');

  $object eq q{} and
    $self->logconfess('A non-empty object argument is required');
  $attribute eq q{} and
    $self->logconfess('A non-empty attribute argument is required');
  $value eq q{} and
    $self->logconfess('A non-empty value argument is required');

  $object = $self->ensure_object_path($object);

  my $avu = $self->make_avu($attribute, $value, $units);
  my $avu_str = $self->avu_str($avu);
  $self->debug("Removing AVU $avu_str from '$object'");

  my @current_meta = $self->get_object_meta($object);
  if (!$self->_meta_exists($attribute, $value, $units, \@current_meta)) {
    $self->logconfess("AVU $avu_str does not exist for '$object'");
  }
  else {
    $self->baton_client->remove_object_avu($object, $attribute, $value,
                                           $units);
    my @remain = grep { not $self->avus_equal($avu, $_) } @current_meta;
    $self->_cache_metadata($object, \@remain);
  }

  return $object;
}

=head2 make_object_avu_history

  Arg [1]    : iRODS data object path.
  Arg [2]    : attribute.
  Arg [3]    : DateTime a timestamp (optional, defaults to the current time).

  Example    : $irods->make_object_avu_history('/my/path/lorem.txt', 'id');
  Description: Return a new history AVU reflecting the current state of
               the attribue. i.e. call this method before you change the
               AVU.

               The history will be of the form:

               [<ISO8601 timestamp>] <value>[,<value>]+

               If there are multiple AVUS for the specified attribute, their
               values will be sorted and concatenated, separated by commas.
               If there are no AVUs specified attribute, an error will be
               raised.
  Returntype : HashRef

=cut

sub make_object_avu_history {
  my ($self, $object, $attribute, $timestamp) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');
  defined $attribute or
    $self->logconfess('A defined attribute argument is required');

  $object eq q{} and
    $self->logconfess('A non-empty object argument is required');
  $attribute eq q{} and
    $self->logconfess('A non-empty attribute argument is required');

  $object = $self->ensure_object_path($object);

  my @historic_avus = grep { $_->{attribute} eq $attribute }
    $self->get_object_meta($object);
  unless (@historic_avus) {
    $self->logconfess("Failed to make a history for attribute '$attribute' ",
                      "on object '$object' because there are no AVUs with ",
                      "that attribute");
  }

  return $self->_make_avu_history($attribute, \@historic_avus, $timestamp);
}

=head2 find_objects_by_meta

  Arg [1]    : iRODS collection path.
  Arg [2]    : ArrayRefs of attribute value tuples.

  Example    : $irods->find_objects_by_meta('/my/path/foo/',
                                            ['id' => 'ABCD1234'])
  Description: Find objects by their metadata, restricted to a parent
               collection. The collection path argument is not a simple
               string prefix, it is a collection. i.e. '/my/path/foo' is
               equivalent to '/my/path/foo/' and will not return results
               in collection '/my/path/foo_1'.
               Return a list of objects, sorted by their data object name
               component.
  Returntype : Array

=cut

sub find_objects_by_meta {
  my ($self, $root, @query_specs) = @_;

  defined $root or $self->logconfess('A defined root argument is required');
  $root eq q{} and $self->logconfess('A non-empty root argument is required');

  $root = $self->ensure_collection_path($root);

  # Ensure a single trailing slash for collection boundary matching.
  $root =~ s/\/*$/\//msx;

  my $zone = $self->find_zone_name($root);
  # baton >= 0.10.0 uses paths as per-query zone hints
  my $zone_path = "/$zone";

  my @avu_specs;
  foreach my $query_spec (@query_specs) {
    my ($attribute, $value, $operator) = @$query_spec;

    my $spec = {attribute => $attribute,
                value     => $value};
    if ($operator) {
      $spec->{operator} = $operator;
    }

    push @avu_specs, $spec;
  }

  my $results = $self->baton_client->search_objects($zone_path, @avu_specs);
  $self->debug("Found ", scalar @$results, " objects (to filter by '$root')");
  my @sorted = sort { $a cmp $b } @$results;
  $self->debug("Sorted ", scalar @sorted, " objects (to filter by '$root')");

  return grep { /^$root/msx } @sorted;
}

=head2 checksum

  Arg [1]    : iRODS data object path.

  Example    : $cs = $irods->checksum('/my/path/lorem.txt')
  Description: Return the MD5 checksum of an iRODS data object. The checksum
               returned is the iRODS cached value, which may be empty if
               the calculation has not yet been done.
  Returntype : Str

=cut

sub checksum {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object eq q{} and
    $self->logconfess('A non-empty object argument is required');

  $object = $self->ensure_object_path($object);

  return $self->baton_client->list_object_checksum($object);
}

=head2 size

  Arg [1]    : iRODS data object path.

  Example    : $cs = $irods->size('/my/path/lorem.txt')
  Description: Return the size in bytes of an iRODS data object. The size
               returned is the iRODS catalog value, which may be different
               from the actual size on disk.
  Returntype : Int

=cut

sub size {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object eq q{} and
    $self->logconfess('A non-empty object argument is required');

  $object = $self->ensure_object_path($object);

  return $self->baton_client->list_object_size($object);
}


=head2 collection_checksums

  Arg [1]    : iRODS collection path.
  Arg [2]    : Recurse, Bool. Optional, defaults to false.

  Example    : $cs = $irods->collection_checksums('/my/path/')
  Description: Return the MD5 checksum of the iRODS data objects in a
               collection as a mapping of data object path to corresponding
               checksum.
  Returntype : HashRef

=cut

sub collection_checksums {
  my ($self, $collection, $recurse) = @_;

  defined $collection or
    $self->logconfess('A defined collection argument is required');

  $collection eq q{} and
    $self->logconfess('A non-empty collection argument is required');

  $collection = $self->ensure_collection_path($collection);

  return $self->baton_client->list_collection_checksums($collection, $recurse);
}

=head2 calculate_checksum

  Arg [1]    : iRODS data object path.

  Example    : $cs = $irods->calculate_checksum('/my/path/lorem.txt')
  Description: Return the MD5 checksum of an iRODS data object. Uses -f
               to force it to be up to date.
  Returntype : Str

=cut

sub calculate_checksum {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object eq q{} and
    $self->logconfess('A non-empty object argument is required');

  $object = $self->ensure_object_path($object);

  my $checksum = $self->baton_client->calculate_object_checksum($object);

  return $checksum;
}

=head2 validate_checksum_metadata

  Arg [1]    : iRODS data object path.

  Example    : $irods->validate_checksum_metadata('/my/path/lorem.txt')
  Description: Return true if the MD5 checksum in the metadata of an iRODS
               object is identical to the MD5 calculated by iRODS.
  Returntype : Bool

=cut

sub validate_checksum_metadata {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object eq q{} and
    $self->logconfess('A non-empty object argument is required');

  $object = $self->ensure_object_path($object);

  my $identical = 0;
  my @md5 = grep { $_->{attribute} eq $FILE_MD5 }
    $self->get_object_meta($object);

  unless (@md5) {
    $self->logconfess("Failed to validate MD5 metadata for '$object' ",
                      "because it is missing");
  }

  if (scalar @md5 > 1) {
    $self->logconfess("Failed to validate MD5 metadata for '$object' ",
                      "because it has multiple values");
  }

  my $avu = $md5[0];
  my $irods_md5 = $self->calculate_checksum($object);
  my $md5 = $avu->{value};

  if ($md5 eq $irods_md5) {
    $self->debug("Confirmed '$object' MD5 as ", $md5);
    $identical = 1;
  }
  else {
    $self->debug("Expected MD5 of $irods_md5 but found $md5 for '$object'");
  }

  return $identical;
}

=head2 replicates

  Arg [1]    : iRODS data object path.

  Example    : my @replicates = $irods->replicates('/my/path/lorem.txt')
  Description: Return an array of all replicate descriptors for a data object.
               Each replicate is represented as a HashRef of the form:
                   {
                     checksum => <checksum Str>,
                     location => <location Str>,
                     number   => <replicate number Int>,
                     resource => <resource name Str>,
                     valid    => <is valid Int>,
                   }

                The checksum of each replicate is reported using the iRODS
                GenQuery API.
  Returntype : Array[Hashref]

=cut

sub replicates {
  my ($self, $object) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object eq q{} and
    $self->logconfess('A non-empty object argument is required');

  $object = $self->ensure_object_path($object);

  return $self->baton_client->list_object_replicates($object);
}

=head2 valid_replicates

  Arg [1]    : iRODS data object path.

  Example    : my @replicates = $irods->valid_replicates('/my/path/lorem.txt')
  Description: Return an array of all valid replicate descriptors for a data
               object, sorted by ascending replicate number.
  Returntype : Array[Hashref]

=cut

sub valid_replicates {
  my ($self, $object) = @_;

  my @valid_replicates = sort { $a->{number} cmp $b->{number} }
    grep { $_->{valid} } $self->replicates($object);

  return @valid_replicates;
}

=head2 invalid_replicates

  Arg [1]    : iRODS data object path.

  Example    : my @replicates = $irods->invalid_replicates('/my/path/lorem.txt')
  Description: Return an array of all invalid replicate descriptors for a data
               object, sorted by ascending replicate number.
  Returntype : Array[Hashref]

=cut

sub invalid_replicates {
  my ($self, $object) = @_;

  my @invalid_replicates = sort { $a->{number} cmp $b->{number} }
    grep { not $_->{valid} } $self->replicates($object);

  return @invalid_replicates;
}

=head2 prune_replicates

  Arg [1]    : iRODS data object path.

  Example    : my @pruned = $irods->prune_replicates('/my/path/lorem.txt')
  Description: Remove any replicates of a data object that are marked as
               stale in the ICAT.  Return an array of descriptors of the
               pruned replicates, sorted by ascending replicate number.
               Each replicate is represented as a HashRef of the form:
                   {
                     checksum => <checksum Str>,
                     location => <location Str>,
                     number   => <replicate number Int>,
                     resource => <resource name Str>,
                     valid    => <is valid Int>,
                   }

               Raise anm error if there are only invalid replicates; there
               should always be a valid replicate and pruning in this case
               would be equivalent to deletion.
  Returntype : Array[Hashref]

=cut

sub prune_replicates {
  my ($self, $object) = @_;

  my @invalid_replicates = $self->invalid_replicates($object);

  my @pruned;
  if ($self->valid_replicates($object)) {
    foreach my $rep (@invalid_replicates) {
      my $resource = $rep->{resource};
      my $checksum = $rep->{checksum};
      my $rep_num  = $rep->{number};
      $self->debug("Pruning invalid replicate $rep_num with checksum ",
                   "'$checksum' from resource '$resource' for ",
                   "data object '$object'");
      $self->remove_replicate($object, $rep_num);
      push @pruned, $rep;
    }
  }
  else {
    $self->logconfess("Failed to prune invalid replicates from '$object': ",
                      "there and no valid replicates of this data object; ",
                      "pruning would be equivalent to deletion");
  }

  return @pruned;
}

=head2 remove_replicate

  Arg [1]    : iRODS data object path.
  Arg [2]    : replicate number

  Example    : $irods->remove_replicate('/my/path/lorem.txt')
  Description: Remove a replicate of a data object.  Return the object path.
  Returntype : Str

=cut

sub remove_replicate {
  my ($self, $object, $replicate_num) = @_;

  defined $object or
    $self->logconfess('A defined object argument is required');

  $object eq q{} and
    $self->logconfess('A non-empty object argument is required');

  $object = $self->ensure_object_path($object);

  $replicate_num =~ m{^\d+$}msx or
    $self->logconfess('A non-negative integer replicate_num argument ',
                      'is required');

  $self->debug("Removing replicate '$replicate_num' of '$object'");
  WTSI::DNAP::Utilities::Runnable->new(executable  => $IRM,
                                       arguments   => ['-n', $replicate_num,
                                                       $object],
                                       environment => $self->environment)->run;
  return $object;
}

=head2 avu_history_attr

  Arg [1]    : iRODS data object path.
  Arg [2]    : attribute.

  Example    : $irods->avu_history_attr('/my/path/lorem.txt', 'id');
  Description: Return the new history AVU attribute corresponding to the
               specified attribute.
  Returntype : Str

=cut

sub avu_history_attr {
  my ($self, $attribute) = @_;

  defined $attribute or
    $self->logconfess('A defined attribute argument is required');

  $attribute eq q{} and
    $self->logconfess('A non-empty attribute argument is required');

  return $attribute . '_history';
}

=head2 is_avu_history_attr

  Arg [1]    : iRODS data object path.
  Arg [2]    : attribute.

  Example    : $irods->is_avu_history_attr('id_history');
  Description: Return true if the attribute string matches the pattern
               expected for an AVU history attribute.
  Returntype : Bool

=cut

sub is_avu_history_attr {
  my ($self, $attribute) = @_;

  defined $attribute or
    $self->logconfess('A defined attribute argument is required');

  $attribute eq q{} and
    $self->logconfess('A non-empty attribute argument is required');

  return $attribute =~ m{.*_history$}msx;
}

sub _build_baton_client {
  my ($self) = @_;

  my @arguments = ('--unbuffered');
  if ($self->single_server) {
    $self->info('Starting the baton client in single-server mode');
    push @arguments, '--single-server';
  }

  return WTSI::NPG::iRODS::BatonClient->new
    (arguments   => \@arguments,
     environment => $self->environment)->start;
}

sub _ensure_absolute_path {
  my ($self, $target) = @_;

  my $absolute = $target;
  unless ($target =~ m{^/}msx) {
    $absolute = $self->working_collection . q{/} . $absolute;
  }

  return $absolute;
}

sub _meta_exists {
  my ($self, $attribute, $value, $units, $current_meta) = @_;

  ref $current_meta eq 'ARRAY' or
    $self->logconfess("current_meta argument must be an ArrayRef");

  if ($units) {
    return grep { $_->{attribute} eq $attribute &&
                  $_->{value}     eq $value     &&
                  $_->{units}     eq $units } @$current_meta;
  }
  else {
    return grep { $_->{attribute} eq $attribute &&
                  $_->{value}     eq $value} @$current_meta;
  }
}

sub _make_avu_history {
  my ($self, $attribute, $historic_avus, $history_timestamp) = @_;

  $self->is_avu_history_attr($attribute) and
    $self->logcroak("An AVU history may not be created for the ",
                    "history attribute '$attribute'");

  $history_timestamp ||= DateTime->now->iso8601;

  my @historic_values = sort { $a cmp $b } map { $_->{value} } @$historic_avus;

  my $history_attribute = $self->avu_history_attr($attribute);
  my $history_value     = sprintf "[%s] %s", $history_timestamp, join q{,},
    @historic_values;

  return {attribute => $history_attribute,
          value     => $history_value,
          units     => undef};
}

sub _build_groups {
  my ($self) = @_;

  return [$self->list_groups];
}

sub _cache_metadata {
  my ($self, $path, $avus) = @_;

  my $sorted = [$self->sort_avus(@{$avus})];
  $self->_metadata_cache->set($path, $sorted);
  $self->debug("Updated AVU cache for '$path': ", pp($sorted));

  return $sorted;
}

sub _cache_permissions {
  my ($self, $path, $acl) = @_;

  # Having 'null' permission means having no permission, so these
  # must not be cached.
  my @to_cache =
    grep { $_->{level} ne $WTSI::NPG::iRODS::NULL_PERMISSION } @{$acl};

  my $sorted = [$self->sort_acl(@to_cache)];
  $self->_permissions_cache->set($path, $sorted);
  $self->debug("Updated ACL cache for '$path': ", pp($sorted));

  return $sorted;
}

sub _clear_caches {
  my ($self, $path) = @_;

  $self->debug("Clearing cached path, AVUs and ACL for '$path'");
  $self->_path_cache->remove($path);
  $self->_permissions_cache->remove($path);
  $self->_metadata_cache->remove($path);

  return;
}

sub DEMOLISH {
  my ($self, $in_global_destruction) = @_;

  # Only do try to stop cleanly if the object is not already being
  # destroyed by Perl (as indicated by the flag passed in by Moose).
  if (not $in_global_destruction) {

    # Stop any active client and log any errors that it encountered
    # while running. This preempts the client being stopped within its
    # own destructor and allows our logger to be resonsible for
    # reporting any errors.
    #
    # If stopping were left to the client destructor, Moose would
    # handle any errors by warning to STDERR instead of using the log.
    if ($self->has_baton_client) {
      try {
        $self->debug("Stopping baton client");
        my $startable = $self->baton_client;

        my $muffled = Log::Log4perl->get_logger('log4perl.logger.Muffled');
        $muffled->level($OFF);
        $startable->logger($muffled);
        $startable->stop;
      } catch {
        $self->error("Failed to stop baton client cleanly: ", $_);
      };
    }
  }

  return;
}

__PACKAGE__->meta->make_immutable;

no Moose;

1;

__END__


=head1 NAME

WTSI::NPG::iRODS

=head1 SYNOPSIS

  my $irods = WTSI::NPG::iRODS->new;
  my $rods_path = $irods->add_object("file.txt", "irods/path");
  print $irods->list_object($rods_path), "\n";

  $irods->add_object_avu(rods_path, 'a', 'b', 'c');
  $irods->add_object_avu(rods_path, 'x', 'y1', 'z');
  $irods->add_object_avu(rods_path, 'x', 'y2', 'z');

  my @objs = $irods->find_objects_by_meta('/',
                                          [a => 'b'],
                                          [y => 'z%', 'like']);

=head1 DESCRIPTION

This class provides access to iRODS operations on data objects,
collections and metadata. It does so by launching several client
programs in the background, each of which holds open a connection

On creation, an instance captures a copy of %ENV which it uses for all
its child processes.

iRODS paths to data objects and collections are represented as
strings. AVUs are represented as HashRefs of the form

  { attribute => <attribute name>,
    value     => <attribute value>,
    units     => <attribute units> }

Units are optional.

Query clauses are represented as ArrayRefs of the form

  [ <attribute name>, <attribute value> <operator> ]

The operator is optional, defaulting to '='. Valid operators are '=',
'like', '<' and '>'.

e.g. The query

  [x => 'a'], [y => 'b'], [z => 'c%', 'like']

is translated to the iRODS imeta query

  x = a and y = b and x like c%

=head1 AUTHOR

Keith James <kdj@sanger.ac.uk>

=head1 COPYRIGHT AND DISCLAIMER

Copyright (C) 2013, 2014, 2015, 2016, 2017, 2021 Genome Research
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
