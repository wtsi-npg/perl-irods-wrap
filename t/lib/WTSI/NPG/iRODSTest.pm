package WTSI::NPG::iRODSTest;

use utf8;

use strict;
use version;
use warnings;

use English qw(-no_match_vars);
use File::Spec;
use File::Temp qw(tempdir);
use List::AllUtils qw(all any none);
use Log::Log4perl;
use Try::Tiny;
use Unicode::Collate;

use base qw(WTSI::NPG::iRODS::Test);
use Test::More;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::NPG::iRODS;

my $pid = $PID;
my $cwc;

my $fixture_counter = 0;
my $data_path = './t/data/irods';
my $irods_tmp_coll;

my $repl_resource = $ENV{WTSI_NPG_iRODS_Test_Repl_Resource};
$repl_resource ||= 'replResc';
my $alt_resource = $ENV{WTSI_NPG_iRODS_Test_Resource};
$alt_resource ||= 'demoResc';

# Prefix for test iRODS data access groups
my $group_prefix = 'ss_';
# Groups to be added to the test iRODS
my @irods_groups = map { $group_prefix . $_ } (0, 10, 100);
# Groups added to the test iRODS in fixture setup
my @groups_added;
# Enable group tests
my $group_tests_enabled = 0;

sub setup_test : Test(setup) {
  my ($self) = @_;

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  $cwc = $irods->working_collection;

  $irods_tmp_coll = $irods->add_collection("iRODSTest.$pid.$fixture_counter");
  $fixture_counter++;

  $irods->add_collection("$irods_tmp_coll/empty");
  $irods->add_collection("$irods_tmp_coll/empty_tree");
  $irods->add_collection("$irods_tmp_coll/empty_tree/1");
  $irods->add_collection("$irods_tmp_coll/empty_tree/2");
  $irods->add_collection("$irods_tmp_coll/empty_tree/3");

  $irods->put_collection($data_path, $irods_tmp_coll);

  my $test_coll = "$irods_tmp_coll/irods";
  my $test_obj = File::Spec->join($test_coll, 'lorem.txt');

  foreach my $attr (qw(a b c)) {
    foreach my $value (qw(x y)) {
      my $units = $value eq 'x' ? 'cm' : undef;
      $irods->add_collection_avu($test_coll, $attr, $value, $units);
      $irods->add_object_avu($test_obj, $attr, $value, $units);
    }
  }

  @groups_added = $self->add_irods_groups($irods, @irods_groups);
  if (scalar @groups_added == scalar @irods_groups) {
    $group_tests_enabled = 1;
  }
}

sub teardown_test : Test(teardown) {
  my ($self) = @_;

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  $irods->working_collection($cwc);
  $irods->remove_collection($irods_tmp_coll);
  $self->remove_irods_groups($irods, @groups_added);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::iRODS');
}

sub compatible_baton_versions : Test(10) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my @incompatible_versions = qw(0.16.0
                                 0.16.1
                                 0.16.2
                                 0.16.3
                                 0.16.4
                                 0.17.0
                                 0.17.1
                                 1.1.0
                                 1.2.0);

  foreach my $version (@incompatible_versions) {
    ok(!$irods->match_baton_version($version),
       "Incompatible with baton $version");
  }

  my @compatible_versions = qw(2.0.0);
  foreach my $version (@compatible_versions) {
    ok($irods->match_baton_version($version),
       "Compatible with baton $version");
  }
}

sub match_baton_version : Test(11) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  {
    local $WTSI::NPG::iRODS::MAX_BATON_VERSION = '1.2.2';
    local $WTSI::NPG::iRODS::MIN_BATON_VERSION = '1.1.1';

    ok(!$irods->match_baton_version('2.2.2'), 'Too new, major version');
    ok(!$irods->match_baton_version('1.3.2',  'Too new, minor version'));
    ok(!$irods->match_baton_version('1.2.3'), 'Too new, patch version');
    ok($irods->match_baton_version('1.2.2'),  'In range, max version');
    ok($irods->match_baton_version('1.2.0'),  'In range version');
    ok($irods->match_baton_version('1.1.1'),  'In range, min version');
    ok(!$irods->match_baton_version('1.1.0'), 'Too old, patch version');
    ok(!$irods->match_baton_version('1.0.1'), 'Too old, minor version');
    ok(!$irods->match_baton_version('0.1.1'), 'Too old, major version');

    ok($irods->match_baton_version('1.2.0-abcdef'));
    ok($irods->match_baton_version('1.2.0-1-abcdef'));
  }
}

sub single_server : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    single_server        => 1,
                                    strict_baton_version => 0);
  ok($irods->single_server, 'Can start in single-server mode');
}

sub group_prefix : Test(6) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  is($irods->group_prefix, 'ss_', 'Default group prefix');

  dies_ok { $irods->group_prefix('') }
    'Failed to set empty group prefix';
  dies_ok { $irods->group_prefix(' ') }
    'Failed to set whitespace group prefix';
  dies_ok { $irods->group_prefix('foo bar') }
    'Failed to set internal whitespace group prefix';

  ok($irods->group_prefix('foo_'), 'Set group prefix');
  is($irods->make_group_name('bar'), 'foo_bar', 'Group prefix used')
}

sub group_filter : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  ok($irods->group_filter->('ss_0'),   'Default group filter include');
  ok(!$irods->group_filter->('public'), 'Default group filter exclude');
}

sub absolute_path : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  is($irods->absolute_path('/path'), '/path');
  is($irods->absolute_path('path'), $irods->working_collection . '/path');
}

sub get_irods_env : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  ok($irods->get_irods_env, 'Obtained an iRODS environment');
  is(ref $irods->get_irods_env, 'HASH', 'iRODS environment is a HashRef');
}

sub get_irods_user : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  ok($irods->get_irods_user, 'Obtained an iRODS user name')
}

sub get_irods_home : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  ok($irods->get_irods_user, 'Obtained an iRODS user name')
}

sub find_zone_name : Test(3) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $wc = $irods->working_collection;
  my ($zone) = $wc =~ m{^/([^/]+)};

  is($irods->find_zone_name($wc), $zone, 'Works for absolute paths');
  is($irods->find_zone_name('/no_such_zone'), 'no_such_zone',
     'Works for non-existent paths');
  is($irods->find_zone_name('relative'), $zone,
     'Falls back to current zone for relative paths');
}

sub working_collection : Test(5) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  like($irods->working_collection, qr{^/}, 'Found a working collection');

  isnt($irods->working_collection, $irods_tmp_coll);
  ok($irods->working_collection($irods_tmp_coll), 'Set the working collection');
  is($irods->working_collection, $irods_tmp_coll, 'Working collection set');

  dies_ok {
    $irods->working_collection('/no_such_collection')
  } 'Expected to fail setting working collection to a non-existent collection';
}

sub list_groups : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  ok(grep { /^rodsadmin$/ } $irods->list_groups, 'Listed the rodsadmin group');
}

sub group_exists : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  ok($irods->group_exists('rodsadmin'), 'The rodsadmin group exists');
  ok(!$irods->group_exists('no_such_group'), 'An absent group does not exist');
}

sub set_group_access : Test(7) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";

  dies_ok { $irods->set_group_access('no_such_permission', 'public',
                                     $lorem_object) }
    'Expected to fail setting access with an invalid permission argument';

  dies_ok { $irods->set_group_access('read', 'no_such_group_exists',
                                     $lorem_object) }
    'Expected to fail setting access for non-existent group';

  my $zone = $irods->find_zone_name($irods_tmp_coll);
  my $r0 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{zone}  && $_->{zone}  eq $zone    &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $irods->get_object_permissions($lorem_object);
  ok($r0, 'No public read access');

  ok($irods->set_group_access('read', 'public', $lorem_object));

  my $r1 = any { exists $_->{owner} && $_->{owner} eq 'public' &&
                 exists $_->{zone}  && $_->{zone}  eq $zone    &&
                 exists $_->{level} && $_->{level} eq 'read' }
    $irods->get_object_permissions($lorem_object);
  ok($r1, 'Added public read access');

  ok($irods->set_group_access(undef, 'public', $lorem_object));

  my $r2 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{zone}  && $_->{zone}  eq $zone    &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $irods->get_object_permissions($lorem_object);
  ok($r2, 'Removed public read access');
}

sub get_object_permissions : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";

  my $perms = all { exists $_->{owner} &&
                    exists $_->{zone}  &&
                    exists $_->{level} }
    $irods->get_object_permissions($lorem_object);
  ok($perms, 'Permissions obtained');
}

sub set_object_permissions : Test(8) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";

  dies_ok { $irods->set_object_permissions('no_such_permission', 'public',
                                           $lorem_object) }
    'Expected to fail setting access with an invalid permission argument';

  dies_ok { $irods->set_object_permissions('read', 'no_such_group_exists',
                                           $lorem_object) }
    'Expected to fail setting access for non-existent group';

  ok($irods->set_object_permissions('read', 'public', $lorem_object),
     'Set object permissions, implicit zone');

  my $zone = $irods->find_zone_name($irods_tmp_coll);
  my $r1 = any { exists $_->{owner} && $_->{owner} eq 'public' &&
                 exists $_->{zone}  && $_->{zone}  eq $zone    &&
                 exists $_->{level} && $_->{level} eq 'read' }
    $irods->get_object_permissions($lorem_object);
  ok($r1, 'Added public read access');

  ok($irods->set_object_permissions(undef, 'public', $lorem_object));

  my $r2 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{zone}  && $_->{zone}  eq $zone    &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $irods->get_object_permissions($lorem_object);
  ok($r2, 'Removed public read access');

  ok($irods->set_object_permissions('read', "public#$zone", $lorem_object),
     'Set object permissions, explicit zone');

 SKIP: {
    my $version = $irods->installed_baton_version;
    my ($dotted_version, $commits) = $version =~ m{^(\d+[.]\d+[.]\d+)(\S*)$}msx;

    skip "baton $version is < 0.16.3", 1 unless
      version->parse($dotted_version) > version->parse('0.16.2');

    dies_ok { $irods->set_object_permissions('read', 'public#no_such_zone',
                                             $lorem_object) }
      'Expected to fail setting access for user in a non-existent zone';
  }
}

sub get_object_groups : Test(7) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";

 SKIP: {
     if (not $group_tests_enabled) {
       skip 'iRODS test groups were not present', 6;
     }

     ok($irods->set_object_permissions('read', 'public', $lorem_object));
     ok($irods->set_object_permissions('read', 'ss_0',   $lorem_object));
     ok($irods->set_object_permissions('read', 'ss_10',  $lorem_object));

     my $expected_all = ['ss_0', 'ss_10'];
     my @found_all  = $irods->get_object_groups($lorem_object);
     is_deeply(\@found_all, $expected_all, 'Expected all groups')
       or diag explain \@found_all;

     my $expected_read = ['ss_0', 'ss_10'];
     my @found_read = $irods->get_object_groups($lorem_object, 'read');
     is_deeply(\@found_read, $expected_read, 'Expected read groups')
       or diag explain \@found_read;

     $irods->group_filter(sub {
                            my ($owner) = @_;
                            if ($owner =~ m{^(public|ss_)}) {
                              return 1;
                            }
                          });
     my $expected_filter = ['public', 'ss_0', 'ss_10'];
     my @found_filter  = $irods->get_object_groups($lorem_object);
     is_deeply(\@found_filter, $expected_filter, 'Expected filtered groups')
       or diag explain \@found_filter;
   }

   my $expected_own = [];
   my @found_own  = $irods->get_object_groups($lorem_object, 'own');
   is_deeply(\@found_own, $expected_own, 'Expected own groups')
     or diag explain \@found_own;
}

sub get_collection_permissions : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $coll = "$irods_tmp_coll/irods";

  my $perms = all { exists $_->{owner} &&
                    exists $_->{zone}  &&
                    exists $_->{level} }
    $irods->get_collection_permissions($coll);
  ok($perms, 'Permissions obtained');
}

sub set_collection_permissions : Test(8) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $coll = "$irods_tmp_coll/irods";

  dies_ok { $irods->set_collection_permissions('no_such_permission', 'public',
                                               $coll) }
    'Expected to fail setting access with an invalid permission argument';

  dies_ok { $irods->set_collection_permissions('read', 'no_such_group_exists',
                                               $coll) }
    'Expected to fail setting access for non-existent group';

  ok($irods->set_collection_permissions('read', 'public', $coll),
     'Set collection permissions, implicit zone');

  my $zone = $irods->find_zone_name($irods_tmp_coll);
  my $r1 = any { exists $_->{owner} && $_->{owner} eq 'public' &&
                 exists $_->{zone}  && $_->{zone}  eq $zone    &&
                 exists $_->{level} && $_->{level} eq 'read' }
    $irods->get_collection_permissions($coll);
  ok($r1, 'Added public read access') or
      diag explain [$irods->get_collection_permissions($coll)];

  ok($irods->set_collection_permissions(undef, 'public', $coll));

  my $r2 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{zone}  && $_->{zone}  eq $zone    &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $irods->get_collection_permissions($coll);
  ok($r2, 'Removed public read access');

  ok($irods->set_collection_permissions('read', "public#$zone", $coll),
     'Set collection permissions, explicit zone');

 SKIP: {
    my $version = $irods->installed_baton_version;
    my ($dotted_version, $commits) = $version =~ m{^(\d+[.]\d+[.]\d+)(\S*)$}msx;

    skip "baton $version is < 0.16.3", 1 unless
      version->parse($dotted_version) > version->parse('0.16.2');

    dies_ok { $irods->set_collection_permissions('read', 'public#no_such_zone',
                                                 $coll) }
      'Expected to fail setting access for user in a non-existent zone';
  }
}

sub get_collection_groups : Test(7) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $coll = "$irods_tmp_coll/irods";

 SKIP: {
    if (not $group_tests_enabled) {
      skip 'iRODS test groups were not present', 6;
    }

    ok($irods->set_collection_permissions('read', 'public', $coll));
    ok($irods->set_collection_permissions('read', 'ss_0',   $coll));
    ok($irods->set_collection_permissions('read', 'ss_10',  $coll));

    my $expected_all = ['ss_0', 'ss_10'];
    my @found_all  = $irods->get_collection_groups($coll);
    is_deeply(\@found_all, $expected_all, 'Expected all groups')
      or diag explain \@found_all;

    my $expected_read = ['ss_0', 'ss_10'];
    my @found_read = $irods->get_collection_groups($coll, 'read');
    is_deeply(\@found_read, $expected_read, 'Expected read groups')
      or diag explain \@found_read;

    $irods->group_filter(sub {
                           my ($owner) = @_;
                           if ($owner =~ m{^(public|ss_)}) {
                             return 1;
                           }
                         });
    my $expected_filter = ['public', 'ss_0', 'ss_10'];
    my @found_filter  = $irods->get_collection_groups($coll);
    is_deeply(\@found_filter, $expected_filter, 'Expected filtered groups')
      or diag explain \@found_filter;
  }

  my $expected_own = [];
  my @found_own  = $irods->get_collection_groups($coll, 'own');
  is_deeply(\@found_own, $expected_own, 'Expected own groups')
    or diag explain \@found_own;
}

sub is_collection : Test(3) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  ok($irods->is_collection($irods_tmp_coll), 'Is a collection');
  ok(!$irods->is_collection("$irods_tmp_coll/irods/lorem.txt"),
     'Object is not a collection');
  ok(!$irods->is_collection('/no_such_collection'),
     'Non-existent path is not a collection');
}

sub list_collection : Test(5) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my ($objs, $colls) = $irods->list_collection("$irods_tmp_coll/irods");

  is_deeply($objs, ["$irods_tmp_coll/irods/lorem.txt",
                    "$irods_tmp_coll/irods/test.txt",
                    "$irods_tmp_coll/irods/utf-8.txt"]) or diag explain $objs;

  is_deeply($colls, ["$irods_tmp_coll/irods",
                     "$irods_tmp_coll/irods/collect_files",
                     "$irods_tmp_coll/irods/md5sum",
                     "$irods_tmp_coll/irods/test"]) or diag explain $colls;

  ok(!$irods->list_collection('no_collection_exists'),
     'Failed to list a non-existent collection');

  my ($objs_deep, $colls_deep) =
    $irods->list_collection("$irods_tmp_coll/irods", 'RECURSE');

  is_deeply($objs_deep, ["$irods_tmp_coll/irods/lorem.txt",
                         "$irods_tmp_coll/irods/test.txt",
                         "$irods_tmp_coll/irods/utf-8.txt",
                         "$irods_tmp_coll/irods/collect_files/a/10.txt",
                         "$irods_tmp_coll/irods/collect_files/a/x/1.txt",
                         "$irods_tmp_coll/irods/collect_files/b/20.txt",
                         "$irods_tmp_coll/irods/collect_files/b/y/2.txt",
                         "$irods_tmp_coll/irods/collect_files/c/30.txt",
                         "$irods_tmp_coll/irods/collect_files/c/z/3.txt",
                         "$irods_tmp_coll/irods/md5sum/lorem.txt",
                         "$irods_tmp_coll/irods/test/file1.txt",
                         "$irods_tmp_coll/irods/test/file2.txt",
                         "$irods_tmp_coll/irods/test/dir1/file3.txt",
                         "$irods_tmp_coll/irods/test/dir2/file4.txt"])
    or diag explain $objs_deep;

  is_deeply($colls_deep, ["$irods_tmp_coll/irods",
                          "$irods_tmp_coll/irods/collect_files",
                          "$irods_tmp_coll/irods/collect_files/a",
                          "$irods_tmp_coll/irods/collect_files/a/x",
                          "$irods_tmp_coll/irods/collect_files/b",
                          "$irods_tmp_coll/irods/collect_files/b/y",
                          "$irods_tmp_coll/irods/collect_files/c",
                          "$irods_tmp_coll/irods/collect_files/c/z",
                          "$irods_tmp_coll/irods/md5sum",
                          "$irods_tmp_coll/irods/test",
                          "$irods_tmp_coll/irods/test/dir1",
                          "$irods_tmp_coll/irods/test/dir2"])
    or diag explain $colls_deep;
}

sub collection_checksums : Test(3) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $checksums = $irods->collection_checksums("$irods_tmp_coll/irods");
  is_deeply($checksums,
            {"$irods_tmp_coll/irods/lorem.txt" =>
             "39a4aa291ca849d601e4e5b8ed627a04",
             "$irods_tmp_coll/irods/test.txt" =>
             "2205e48de5f93c784733ffcca841d2b5",
             "$irods_tmp_coll/irods/utf-8.txt" =>
             "500cec3fbb274064e2a25fa17a69638a"
             }) or diag explain $checksums;

  dies_ok{ $irods->collection_checksums('no_collection_exists') }
    'Failed to list checksums in a non-existent collection';

  my $checksums_deep = $irods->collection_checksums("$irods_tmp_coll/irods",
                                                    'RECURSE');
  is_deeply($checksums_deep,
            {"$irods_tmp_coll/irods/lorem.txt" =>
             "39a4aa291ca849d601e4e5b8ed627a04",
             "$irods_tmp_coll/irods/test.txt" =>
             "2205e48de5f93c784733ffcca841d2b5",
             "$irods_tmp_coll/irods/utf-8.txt" =>
             "500cec3fbb274064e2a25fa17a69638a",
             "$irods_tmp_coll/irods/collect_files/a/10.txt" =>
             "31d30eea8d0968d6458e0ad0027c9f80",
             "$irods_tmp_coll/irods/collect_files/a/x/1.txt" =>
             "b026324c6904b2a9cb4b88d6d61c81d1",
             "$irods_tmp_coll/irods/collect_files/b/20.txt" =>
             "dbbf8220893d497d403bb9cdf49db7a4",
             "$irods_tmp_coll/irods/collect_files/b/y/2.txt" =>
             "26ab0db90d72e28ad0ba1e22ee510510",
             "$irods_tmp_coll/irods/collect_files/c/30.txt" =>
             "d5b4c7d9b06b60a7846c4529834c9812",
             "$irods_tmp_coll/irods/collect_files/c/z/3.txt" =>
             "6d7fce9fee471194aa8b5b6e47267f03",
             "$irods_tmp_coll/irods/md5sum/lorem.txt" =>
             "39a4aa291ca849d601e4e5b8ed627a04",
             "$irods_tmp_coll/irods/test/file1.txt" =>
             "5149d403009a139c7e085405ef762e1a",
             "$irods_tmp_coll/irods/test/file2.txt" =>
             "3d709e89c8ce201e3c928eb917989aef",
             "$irods_tmp_coll/irods/test/dir1/file3.txt" =>
             "60b91f1875424d3b4322b0fdd0529d5d",
             "$irods_tmp_coll/irods/test/dir2/file4.txt" =>
             "857c6673d7149465c8ced446769b523c"
            })
    or diag explain $checksums_deep;
}

sub add_collection : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  # Deliberate spaces in names
  my $coll = "$irods_tmp_coll/add_ _collection";
  is($irods->add_collection($coll), $coll, 'Created a collection');
  ok($irods->list_collection($coll), 'Listed a new collection');
}

sub put_collection : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $dir = File::Spec->catdir($data_path, 'test');
  my $target = "$irods_tmp_coll/put_collection";
  $irods->add_collection($target);

  is($irods->put_collection($dir, $target), "$target/test",
     'Put a new collection');

  my @contents = $irods->list_collection("$target/test");

  is_deeply(\@contents,
            [["$irods_tmp_coll/put_collection/test/file1.txt",
              "$irods_tmp_coll/put_collection/test/file2.txt"],

             ["$irods_tmp_coll/put_collection/test",
              "$irods_tmp_coll/put_collection/test/dir1",
              "$irods_tmp_coll/put_collection/test/dir2"]])
    or diag explain \@contents;
}

sub move_collection : Test(5) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $coll_to_move = "$irods_tmp_coll/irods";
  my $coll_moved = "$irods_tmp_coll/irods_moved";

  is($irods->move_collection($coll_to_move, $coll_moved), $coll_moved,
     'Moved a collection');

  ok(!$irods->list_collection($coll_to_move), 'Collection was moved 1');
  ok($irods->list_collection($coll_moved), 'Collection was moved 2');

  dies_ok { $irods->move_collection($coll_to_move, undef) }
    'Failed to move a collection to an undefined place';
  dies_ok { $irods->move_collection(undef, $coll_moved) }
    'Failed to move an undefined collection';
}

sub get_collection : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $coll = "$irods_tmp_coll/irods";
  my $tmpdir = tempdir(CLEANUP => 1);

  ok($irods->get_collection($coll, $tmpdir), 'Got a collection');
  ok(-d "$tmpdir/irods", 'Collection was downloaded');

  dies_ok { $irods->get_collection('/no_such_collection', $tmpdir) }
    'Failed to download a non-existent collection';
  dies_ok { $irods->get_collection(undef, $tmpdir) }
    'Failed to download an undefined collection';
}

sub remove_collection : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $coll = "$irods_tmp_coll/irods";
  is($irods->remove_collection($coll), $coll, 'Removed a collection');
  ok(!$irods->list_collection($coll), 'Collection was removed');

  dies_ok { $irods->remove_collection('/no_such_collection') }
    'Failed to remove a non-existent collection';
  dies_ok { $irods->remove_collection }
    'Failed to remove an undefined collection';
}

sub remove_collection_safely : Test(7) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $empty_coll = "$irods_tmp_coll/empty";
  is($irods->remove_collection_safely($empty_coll), $empty_coll,
     'Removed an empty collection');
  ok(!$irods->list_collection($empty_coll),
     'Empty collection was removed');

  my $empty_tree = "$irods_tmp_coll/empty_tree";
  is($irods->remove_collection_safely($empty_tree), $empty_tree,
     'Removed an empty tree');
  ok(!$irods->list_collection($empty_tree),
     'Empty tree was removed');

  dies_ok { $irods->remove_collection_safely('/no_such_collection') }
          'Failed to remove a non-existent collection';
  dies_ok { $irods->remove_collection_safely }
          'Failed to remove an undefined collection';
  dies_ok { $irods->remove_collection_safely($irods_tmp_coll) }
          'Failed to remove a non-empty tree';
}

sub get_collection_meta : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $coll = "$irods_tmp_coll/irods";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'b', value => 'y'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'}];

  my @observed_meta = sort { $a->{attribute} cmp $b->{attribute} ||
                             $a->{value}     cmp $b->{value} }
    $irods->get_collection_meta($coll);

  is_deeply(\@observed_meta, $expected_meta,
            'Collection metadata found') or diag explain \@observed_meta;

  dies_ok { $irods->get_collection_meta('/no_such_collection',
                                        'attr', 'value') }
          'Failed to get metadata from a non-existent collection';
}


sub add_collection_avu : Test(11) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $num_attrs = 8;
  my %meta = map { 'cattr' . $_ => 'cval' . $_ } 0 .. $num_attrs;

  my $test_coll = $irods_tmp_coll;
  foreach my $attr (keys %meta) {
    is($irods->add_collection_avu($test_coll, $attr, $meta{$attr}),
       $test_coll);
  }

  my $expected_meta = [{attribute => 'cattr0', value => 'cval0'},
                       {attribute => 'cattr1', value => 'cval1'},
                       {attribute => 'cattr2', value => 'cval2'},
                       {attribute => 'cattr3', value => 'cval3'},
                       {attribute => 'cattr4', value => 'cval4'},
                       {attribute => 'cattr5', value => 'cval5'},
                       {attribute => 'cattr6', value => 'cval6'},
                       {attribute => 'cattr7', value => 'cval7'},
                       {attribute => 'cattr8', value => 'cval8'}];

  my @observed_meta = sort { $a->{attribute} cmp $b->{attribute} ||
                             $a->{value}     cmp $b->{value} }
    $irods->get_collection_meta($test_coll);

  is_deeply(\@observed_meta, $expected_meta,
            'Collection metadata added') or diag explain \@observed_meta;

  dies_ok { $irods->add_collection_avu('/no_such_collection',
                                        'attr', 'value') }
    'Failed to add metadata to a non-existent collection';
}

sub remove_collection_avu : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $coll = "$irods_tmp_coll/irods";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'}];

  is($irods->remove_collection_avu($coll, 'b', 'x', 'cm'), $coll);
  is($irods->remove_collection_avu($coll, 'b', 'y'), $coll);

  my @observed_meta = sort { $a->{attribute} cmp $b->{attribute} ||
                             $a->{value}     cmp $b->{value} }
    $irods->get_collection_meta($coll);

  is_deeply(\@observed_meta, $expected_meta,
            'Removed metadata found') or diag explain \@observed_meta;

  dies_ok { $irods->remove_collecion_meta('/no_such_collection'
                                          , 'attr', 'value') }
    'Failed to remove metadata from a non-existent collection';
}

sub make_collection_avu_history : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $coll = "$irods_tmp_coll/irods";
  my $timestamp_regex = '\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\]';

  foreach my $attr (qw(a b c)) {
    like($irods->make_collection_avu_history($coll, $attr)->{value},
         qr{^$timestamp_regex x,y}, "History of $attr");
  }

  dies_ok {
    $irods->make_collection_avu_history($coll, 'no_such_attribute');
  }
}

sub find_collections_by_meta : Test(8) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $expected_coll = "$irods_tmp_coll/irods";

  is_deeply([$irods->find_collections_by_meta($irods_tmp_coll,
                                              ['a', 'x'])],
            [$expected_coll]);

  is_deeply([$irods->find_collections_by_meta($irods_tmp_coll,
                                              ['a', 'x', '='])],
            [$expected_coll]);

  is_deeply([$irods->find_collections_by_meta($irods_tmp_coll,
                                              ['a', 'x'], ['a', 'y'])],
            [$expected_coll]);

  dies_ok {
    $irods->find_collections_by_meta($irods_tmp_coll . q{no_such_collection},
                                     ['a', 'x'])
  } 'Expected to fail using non-existent query root';

  my $new_coll = "$irods_tmp_coll/irods/new";
  ok($irods->add_collection($new_coll));
  ok($irods->add_collection_avu($new_coll, 'a', 'x99'));

  is_deeply([$irods->find_collections_by_meta($irods_tmp_coll,
                                              ['a', 'x%', 'like'])],
            [$expected_coll, $new_coll]);

  dies_ok { $irods->find_collections_by_meta($irods_tmp_coll,
                                             ["a", "x", 'invalid_operator']) }
    'Expected to fail using an invalid query operator';
}

sub is_object : Test(3) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  ok(!$irods->is_object($irods_tmp_coll), 'Collection not an object');
  ok($irods->is_object("$irods_tmp_coll/irods/lorem.txt"), 'Is an object');
  ok(!$irods->is_object('/no_such_object'),
     'Non-existent path is not an object');
}

sub list_object : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $lorem_file = "$data_path/lorem.txt";
  my $lorem_object = "$irods_tmp_coll/lorem.txt";
  $irods->add_object($lorem_file, $lorem_object);

  is($irods->list_object($lorem_object), $lorem_object);

  my $lorem = "$irods_tmp_coll/irods/lorem.txt";
  is($irods->list_object($lorem), $lorem);

  ok(!$irods->list_object('no_object_exists'),
     'Failed to list a non-existent object');

  dies_ok { $irods->list_object }
    'Failed to list an undefined object';
}

sub read_object : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $lorem_file = "$data_path/lorem.txt";
  my $lorem_object = "$irods_tmp_coll/lorem.txt";
  $irods->add_object($lorem_file, $lorem_object);

  my $content = $irods->read_object($lorem_object);
  ok($content, 'Read some object content');

  my $expected = '';
  {
    local $/ = undef;
    open my $fin, "<:encoding(utf8)", $lorem_file or die "Failed to open $!";
    $expected = <$fin>;
    close $fin;
  };

  ok(Unicode::Collate->new->eq($content, $expected),
     'Read expected object contents') or diag explain $content;
}

sub add_object : Test(9) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $lorem_file = "$data_path/lorem.txt";

  my $implicit_path = "$irods_tmp_coll/lorem.txt";
  is($irods->add_object($lorem_file, $irods_tmp_coll), $implicit_path,
     'Added a data object with a collection target path');
  is($irods->list_object($implicit_path), $implicit_path,
     'Found the new data object with an implicit path');

  my $explicit_path = "$irods_tmp_coll/lorem_added.txt";
  is($irods->add_object($lorem_file, $explicit_path,
                        $WTSI::NPG::iRODS::CALC_CHECKSUM), $explicit_path,
     'Added a data object with an object target path');
  is($irods->list_object($explicit_path), $explicit_path,
     'Found the new data object with an explicit path');

  is($irods->checksum($explicit_path), '39a4aa291ca849d601e4e5b8ed627a04',
       'Checksum created on request');

  my $lorem_object_no_checksum = "$irods_tmp_coll/lorem_added_no_checksum.txt";
  is($irods->add_object($lorem_file, $lorem_object_no_checksum),
     $lorem_object_no_checksum,
     'Added a data object without checksum calculation');

  is($irods->checksum($lorem_object_no_checksum), undef,
       'Checksum not created by default');

  dies_ok { $irods->add_object }
    'Failed to add an undefined object';
  dies_ok { $irods->add_object($lorem_file, $explicit_path,
                               'invalid checksum action') }
    'Failed on invalid checksum option';
}

sub replace_object : Test(14) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $lorem_file = "$data_path/lorem.txt";
  my $to_replace = "$irods_tmp_coll/lorem_to_replace.txt";

  my $tmp = File::Temp->new;
  my $empty_file = $tmp->filename;

  $irods->add_object($lorem_file, $to_replace);
  my $checksum_before = $irods->calculate_checksum($to_replace);

  is($irods->replace_object($empty_file, $to_replace), $to_replace,
     'Replaced a data object');

  my $checksum_after = $irods->checksum($to_replace);
  ok($checksum_after, 'Checksum created by default');
  isnt($checksum_after, $checksum_before, 'Data object was replaced');
  is($checksum_after, 'd41d8cd98f00b204e9800998ecf8427e',
    'Data object was replaced with an empty file');

  ok($irods->replace_object($empty_file, $to_replace,
                         $WTSI::NPG::iRODS::SKIP_CHECKSUM));
  ok($irods->checksum($to_replace), 'checksum exists in case ' .
     'when it existed for the original file');

  my $to_replace_no_checksum =
    "$irods_tmp_coll/lorem_to_replace_no_checksum.txt";
  $irods->add_object($lorem_file, $to_replace_no_checksum,
                     $WTSI::NPG::iRODS::SKIP_CHECKSUM);
  is($irods->checksum($to_replace_no_checksum),
    undef,'checksum is not created');

  is($irods->replace_object($empty_file, $to_replace_no_checksum,
                            $WTSI::NPG::iRODS::SKIP_CHECKSUM),
     $to_replace_no_checksum, 'Replaced a data object without checksum');

  is($irods->checksum($to_replace_no_checksum), undef,
       'Checksum not created');

  ok($irods->replace_object($empty_file, $to_replace_no_checksum));
  ok(!$irods->checksum($to_replace_no_checksum),
     'Checksum is not created by default if the replaced object ' .
     'did not have a checksum');

  $to_replace = "$irods_tmp_coll/lorem_to_replace.txt";

  dies_ok { $irods->replace_object($lorem_file, undef) }
    'Failed to replace an undefined object';
  dies_ok { $irods->replace_object(undef, $to_replace) }
    'Failed to replace an object with an undefined file';
  dies_ok { $irods->replace_object($empty_file, $to_replace,
                                   'invalid checksum action') }
    'Failed on invalid checksum option';
}

sub copy_object : Test(16) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $num_attrs = 8;
  my %meta = map { 'dattr' . $_ => 'dval' . $_ } 0 .. $num_attrs;

  my $lorem_file = "$data_path/lorem.txt";
  my $object_to_copy = "$irods_tmp_coll/lorem_to_copy.txt";
  my $object_copied = "$irods_tmp_coll/lorem_copied.txt";
  $irods->add_object($lorem_file, $object_to_copy);

  foreach my $attr (keys %meta) {
    is($irods->add_object_avu($object_to_copy, $attr, $meta{$attr}),
       $object_to_copy);
  }

  my $expected_meta = [{attribute => 'copy_dattr0', value => 'dval0'},
                       {attribute => 'copy_dattr1', value => 'dval1'},
                       {attribute => 'copy_dattr2', value => 'dval2'},
                       {attribute => 'copy_dattr3', value => 'dval3'},
                       {attribute => 'copy_dattr4', value => 'dval4'},
                       {attribute => 'copy_dattr5', value => 'dval5'},
                       {attribute => 'copy_dattr6', value => 'dval6'},
                       {attribute => 'copy_dattr7', value => 'dval7'},
                       {attribute => 'copy_dattr8', value => 'dval8'}];

  my $translator = sub { 'copy_' . $_[0] };

  is($irods->copy_object($object_to_copy, $object_copied, $translator),
     $object_copied, 'Copied a data object');

  ok($irods->list_object($object_to_copy), 'Object was copied 1');
  ok($irods->list_object($object_copied),  'Object was copied 2');

  my @observed_meta = sort { $a->{attribute} cmp $b->{attribute} ||
                             $a->{value}     cmp $b->{value} }
    $irods->get_object_meta($object_copied);

  is_deeply(\@observed_meta, $expected_meta,
            'Copied object metadata found') or diag explain \@observed_meta;

  dies_ok { $irods->copy_object($object_to_copy, undef) }
    'Failed to copy an object to an undefined place';
  dies_ok { $irods->copy_object(undef, $object_copied) }
    'Failed to copy an undefined object';

  $irods->add_collection("$irods_tmp_coll/dest/");
  dies_ok {
    $irods->copy_object($object_to_copy, "$irods_tmp_coll/dest/"),
  } 'Expected to fail copying data object to collection';
}

sub move_object : Test(5) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $lorem_file = "$data_path/lorem.txt";
  my $object_to_move = "$irods_tmp_coll/lorem_to_move.txt";
  my $object_moved = "$irods_tmp_coll/lorem_moved.txt";
  $irods->add_object($lorem_file, $object_to_move);

  is($irods->move_object($object_to_move, $object_moved), $object_moved,
     'Moved a data object');

  ok(!$irods->list_object($object_to_move), 'Object was moved 1');
  ok($irods->list_object($object_moved), 'Object was moved 2');

  dies_ok { $irods->move_object($object_to_move, undef) }
    'Failed to move an object to an undefined place';
  dies_ok { $irods->move_object(undef, $object_moved) }
    'Failed to move an undefined object';
}

sub get_object : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
  my $tmpdir = tempdir(CLEANUP => 1);

  ok($irods->get_object($lorem_object, $tmpdir), 'Got an object');
  ok(-f "$tmpdir/lorem.txt", 'Object was downloaded');

  dies_ok { $irods->get_object('/no_such_object', $tmpdir) }
    'Failed to download a non-existent object';
  dies_ok { $irods->get_object(undef, $tmpdir) }
    'Failed to download an undefined object';
}

sub remove_object : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
  is($irods->remove_object($lorem_object), $lorem_object,
     'Removed a data object');
  ok(!$irods->list_object($lorem_object), 'Object was removed');

  dies_ok { $irods->remove_object('no_such_object') }
    'Failed to remove a non-existent object';
  dies_ok { $irods->remove_object }
    'Failed to remove an undefined object';
}

sub get_object_meta : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'b', value => 'y'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'}];

  my @observed_meta = sort { $a->{attribute} cmp $b->{attribute} ||
                             $a->{value}     cmp $b->{value} }
    $irods->get_object_meta($lorem_object);

  is_deeply(\@observed_meta, $expected_meta,
            'Object metadata found') or diag explain \@observed_meta;

  dies_ok { $irods->get_object_meta('/no_such_object', 'attr', 'value') }
    'Failed to get metadata from a non-existent object';
}

sub add_object_avu : Test(11) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $num_attrs = 8;
  my %meta = map { 'dattr' . $_ => 'dval' . $_ } 0 .. $num_attrs;

  my $test_object = "$irods_tmp_coll/irods/test.txt";
  foreach my $attr (keys %meta) {
    is($irods->add_object_avu($test_object, $attr, $meta{$attr}),
       $test_object);
  }

  my $expected_meta = [{attribute => 'dattr0', value => 'dval0'},
                       {attribute => 'dattr1', value => 'dval1'},
                       {attribute => 'dattr2', value => 'dval2'},
                       {attribute => 'dattr3', value => 'dval3'},
                       {attribute => 'dattr4', value => 'dval4'},
                       {attribute => 'dattr5', value => 'dval5'},
                       {attribute => 'dattr6', value => 'dval6'},
                       {attribute => 'dattr7', value => 'dval7'},
                       {attribute => 'dattr8', value => 'dval8'}];

  my @observed_meta = sort { $a->{attribute} cmp $b->{attribute} ||
                             $a->{value}     cmp $b->{value} }
    $irods->get_object_meta($test_object);

  is_deeply(\@observed_meta, $expected_meta,
            'Object metadata added') or diag explain \@observed_meta;

  dies_ok { $irods->add_object_avu('/no_such_object', 'attr', 'value') }
    'Failed to add metadata to non-existent object';
}

sub remove_object_avu : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'}];

  is($irods->remove_object_avu($lorem_object, 'b', 'x', 'cm'),
     $lorem_object);
  is($irods->remove_object_avu($lorem_object, 'b', 'y'),
     $lorem_object);

  my @observed_meta = sort { $a->{attribute} cmp $b->{attribute} ||
                             $a->{value}     cmp $b->{value} }
    $irods->get_object_meta($lorem_object);

  is_deeply(\@observed_meta, $expected_meta,
            'Removed metadata found') or diag explain \@observed_meta;

  dies_ok { $irods->remove_object_avu('/no_such_object', 'attr', 'value') }
    'Failed to remove metadata from a non-existent object';
}

sub make_object_avu_history : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
  my $timestamp_regex = '\[\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\]';

  foreach my $attr (qw(a b c)) {
    like($irods->make_object_avu_history($lorem_object, $attr)->{value},
         qr{^$timestamp_regex x,y}, "History of $attr");
  }

  dies_ok {
    $irods->make_object_avu_history($lorem_object, 'no_such_attribute');
  }
}

sub add_remove_perl_false_avu : Test(8) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
  ok($irods->add_object_avu($lorem_object, 0, 0, 0),
     'Add 0 int object AVU');
  ok($irods->remove_object_avu($lorem_object, 0, 0, 0),
     'Remove 0 int object AVU');

  ok($irods->add_object_avu($lorem_object, '0', '0', '0'),
     'Add 0 string object AVU');
  ok($irods->remove_object_avu($lorem_object, '0', '0', '0'),
     'Remove 0 string object AVU');

  my $test_coll = $irods_tmp_coll;
  ok($irods->add_collection_avu($test_coll, 0, 0, 0),
     'Add 0 int collection AVU');
  ok($irods->remove_collection_avu($test_coll, 0, 0, 0),
     'Remove 0 int collection AVU');

  ok($irods->add_collection_avu($test_coll, '0', '0', '0'),
     'Add 0 string collection AVU');
  ok($irods->remove_collection_avu($test_coll, '0', '0', '0'),
     'Remove 0 string collection AVU');
}

sub find_objects_by_meta : Test(7) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";

  is_deeply([$irods->find_objects_by_meta($irods_tmp_coll, ['a', 'x'])],
            [$lorem_object]);

  is_deeply([$irods->find_objects_by_meta($irods_tmp_coll, ['a', 'x', '='])],
            [$lorem_object]);

  is_deeply([$irods->find_objects_by_meta($irods_tmp_coll,
                                          ['a', 'x'], ['a', 'y'])],
            [$lorem_object]);

  dies_ok {
    $irods->find_objects_by_meta($irods_tmp_coll . 'no_such_collection',
                                 ['a', 'x'])
  } 'Expected to fail using non-existent query root';

  my $object = "$irods_tmp_coll/irods/test.txt";
  ok($irods->add_object_avu($object, 'a', 'x99'));

  is_deeply([$irods->find_objects_by_meta($irods_tmp_coll,
                                          ['a', 'x%', 'like'])],
            [$lorem_object, $object]);

  dies_ok { $irods->find_objects_by_meta($irods_tmp_coll,
                                         ["a", "x", 'invalid_operator']) }
    'Expected to fail using an invalid query operator';
}

sub checksum : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
  my $expected_checksum = '39a4aa291ca849d601e4e5b8ed627a04';

  #########################################################
  $irods->calculate_checksum($lorem_object);

  is($irods->checksum($lorem_object), $expected_checksum,
     'Checksum matched');
}

sub calculate_checksum : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
  my $expected_checksum = '39a4aa291ca849d601e4e5b8ed627a04';

  is($irods->calculate_checksum($lorem_object), $expected_checksum,
     'Calculated checksum matched');
}

sub validate_checksum_metadata : Test(8) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
  my $expected_checksum = '39a4aa291ca849d601e4e5b8ed627a04';
  my $invalid_checksum = 'xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx';

  dies_ok { $irods->validate_checksum_metadata($lorem_object) }
    "Validation fails without metadata";

  ok($irods->add_object_avu($lorem_object, 'md5', $invalid_checksum));
  ok(!$irods->validate_checksum_metadata($lorem_object));
  ok($irods->remove_object_avu($lorem_object, 'md5', $invalid_checksum));

  ok($irods->add_object_avu($lorem_object, 'md5', $expected_checksum));
  ok($irods->validate_checksum_metadata($lorem_object), 'AVU checksum matched');

  ok($irods->add_object_avu($lorem_object, 'md5', $invalid_checksum));
  dies_ok { $irods->validate_checksum_metadata($lorem_object) }
    "Validation fails with multiple metadata values";
}

sub replicates : Test(9) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0);
  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
  my $expected_checksum = '39a4aa291ca849d601e4e5b8ed627a04';

  system("ichksum -a $lorem_object >/dev/null") == 0
    or die "Failed to update checksum on replicates of $lorem_object: $ERRNO";

  my @replicates = $irods->replicates($lorem_object);
  cmp_ok(scalar @replicates, '==', 2, 'Two replicates are present');

  foreach my $replicate (@replicates) {
    my $num = $replicate->{number};
    is($replicate->{checksum}, $expected_checksum,
      "Replicate $num checksum is correct");
    cmp_ok(length $replicate->{location}, '>', 0,
      "Replicate $num has a location");
    cmp_ok(length $replicate->{resource}, '>', 0,
      "Replicate $num has a resource");
    ok($replicate->{valid}, "Replicate $num is valid");
  }
}

sub invalid_replicates : Test(5) {

 SKIP: {
    if (system("ilsresc $alt_resource >/dev/null") != 0) {
      skip "iRODS resource $alt_resource is unavailable", 3;
    }

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0);

    my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
    my $expected_checksum = '39a4aa291ca849d601e4e5b8ed627a04';
    system("ichksum -f -a $lorem_object >/dev/null") == 0
      or die "Failed to update checksum on replicates of $lorem_object: $ERRNO";

    # Make the original replicates on the replication resource stale
    my $other_object = "$data_path/test.txt";
    system("irepl -S $repl_resource -R $alt_resource $lorem_object >/dev/null") == 0
     or die "Failed to replicate $lorem_object from " .
            "$repl_resource to $alt_resource: $ERRNO";
    system("iput -f -R $alt_resource $other_object $lorem_object >/dev/null") == 0 or
      die "Failed to update a replicate of $lorem_object on $alt_resource: $ERRNO";

    my @invalid_replicates = $irods->invalid_replicates($lorem_object);
    cmp_ok(scalar @invalid_replicates, '==', 2,
           'Two invalid replicate present in the replication resource');

    foreach my $replicate (@invalid_replicates) {
      is($replicate->{checksum}, $expected_checksum,
        'Invalid replicate checksum is correct') or
        diag explain $replicate;
      ok(!$replicate->{valid}, 'Invalid replicate is not valid') or
        diag explain $replicate;
    }
  }
}

sub prune_replicates : Test(8) {

  SKIP: {
    if (system("ilsresc $alt_resource >/dev/null") != 0) {
      skip "iRODS resource $alt_resource is unavailable", 6;
    }

    my $irods = WTSI::NPG::iRODS->new(environment => \%ENV,
      strict_baton_version                        => 0);

    my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
    my $expected_checksum = '39a4aa291ca849d601e4e5b8ed627a04';

    # system("irepl $lorem_object -R $alt_resource >/dev/null") == 0
    #   or die "Failed to replicate $lorem_object to $alt_resource: $ERRNO";
    system("ichksum -f -a $lorem_object >/dev/null") == 0
      or die "Failed to update checksum on replicates of $lorem_object: $ERRNO";

    # Make the original replicates on the replication resource stale
    my $other_object = "$data_path/test.txt";
    system("irepl -S $repl_resource -R $alt_resource $lorem_object >/dev/null") == 0
      or die "Failed to replicate $lorem_object from " .
             "$repl_resource to $alt_resource: $ERRNO";
    system("iput -f -R $alt_resource $other_object $lorem_object >/dev/null") == 0 or
      die "Failed to update a replicate of $lorem_object on $alt_resource:
$ERRNO";

    my @pruned_replicates = $irods->prune_replicates($lorem_object);
    cmp_ok(scalar @pruned_replicates, '==', 2,
      'Two pruned replicates are present');

    foreach my $pruned_replicate (@pruned_replicates) {
      is($pruned_replicate->{checksum}, $expected_checksum,
        "Pruned replicate checksum is correct");
      ok(!$pruned_replicate->{valid}, 'Pruned replicate is not valid') or
        diag explain $pruned_replicate;
    }

    my @replicates = $irods->valid_replicates($lorem_object);
    cmp_ok(scalar @replicates, '==', 1, 'One valid replicate remains');
    my $replicate = $replicates[0];
    isnt($replicate->{checksum}, $expected_checksum,
      'Remaining valid replicate checksum has changed') or
      diag explain $replicate;
    ok($replicate->{valid}, 'Remaining valid replicate is valid') or
      diag explain $replicate;
  }
}

sub md5sum : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  is($irods->md5sum("$data_path/md5sum/lorem.txt"),
     '39a4aa291ca849d601e4e5b8ed627a04');
}

sub hash_path : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  is($irods->hash_path("$data_path/md5sum/lorem.txt"), '39/a4/aa');

  is($irods->hash_path("$data_path/md5sum/lorem.txt",
                       'aabbccxxxxxxxxxxxxxxxxxxxxxxxxxx'), 'aa/bb/cc');
}

sub avu_history_attr : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  is($irods->avu_history_attr('foo'), 'foo_history', 'History attribute');

  dies_ok {
    $irods->avu_history_attr('');
  } 'History attribute empty'
}

sub is_avu_history_attr : Test(3) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  ok($irods->is_avu_history_attr('foo_history'), 'Is history attribute');
  ok(!$irods->is_avu_history_attr('foo'), 'Is not history attribute');

  dies_ok {
    $irods->is_avu_history_attr('');
  } 'Is history attribute empty'
}

sub round_trip_utf8_avu : Test(5) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $attr  = "Τη γλώσσα μου έδωσαν ελληνική";
  my $value = "το σπίτι φτωχικό στις αμμουδιές του Ομήρου";

  my $test_object = "$irods_tmp_coll/irods/test.txt";
  my @meta_before = $irods->get_object_meta($test_object);
  cmp_ok(scalar @meta_before, '==', 0, 'No AVUs present');

  my $expected_meta = [{attribute => $attr, value => $value}];
  ok($irods->add_object_avu($test_object, $attr, $value), 'UTF-8 AVU added');

  my @meta_after = $irods->get_object_meta($test_object);
  cmp_ok(scalar @meta_after, '==', 1, 'One AVU added');

  my $avu = $meta_after[0];

  ok(Unicode::Collate->new->eq($avu->{attribute}, $attr),
     'Found UTF-8 attribute');
  ok(Unicode::Collate->new->eq($avu->{value}, $value),
     'Found UTF-8 value');
}

sub slurp_object : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $test_file = "$data_path/utf-8.txt";
  my $test_object = "$irods_tmp_coll/irods/utf-8.txt";

  my $data = $irods->slurp_object($test_object);

  my $original = '';
  {
    local $/ = undef;
    open my $fin, '<:encoding(utf-8)', $test_file or die "Failed to open $!\n";
    $original = <$fin>;
    close $fin;
  }

  ok(Unicode::Collate->new->eq($data, $original), 'Slurped copy is identical');
}

sub make_avu : Test(6) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $expected1 = {attribute => 'a', value => 'b', units => 'c'};
  my $avu1 = $irods->make_avu('a', 'b', 'c');
  is_deeply($avu1, $expected1, 'AVU with units') or diag explain $avu1;

  my $expected2 = {attribute => 'a', value => 'b'};
  my $avu2 = $irods->make_avu('a', 'b');
  is_deeply($avu2, $expected2, 'AVU without units') or diag explain $avu2;

  dies_ok {
    $irods->make_avu(undef, 'b');
  } 'AVU must have a defined attribute';

  dies_ok {
    $irods->make_avu('a', undef);
  } 'AVU must have a defined value';

  dies_ok {
    $irods->make_avu(q{}, 'b');
  } 'AVU must have a non-empty attribute';

  dies_ok {
    $irods->make_avu('a', q{});
  } 'AVU must have a non-empty value';
}

sub make_avus_from_objects: Test(4) {

  {
    package WTSI::NPG::DeepThought;

    use Moose;

    sub answer {
      return 42;
    }

    sub sum_if_even {
      # return the sum of two arguments, if the sum is even; undef otherwise
      my ($self, $num1, $num2) = @_;
      my $sum = $num1 + $num2;
      if ($sum % 2 == 0) { return $sum; }
      else { return undef; }
    }

    no Moose;
    1;
  }

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my @objs;
  for (1 .. 3) { push @objs, WTSI::NPG::DeepThought->new(); }
  my $args1 = [[1,3], [3,5], [3,6]];
  # no attribute for arguments [3,6] because sum_if_even returns undef
  my @expected1 = (
    {attribute => 'a', value => 4, units => 'florins'},
    {attribute => 'a', value => 8, units => 'florins'});
  my @avus1 = $irods->make_avus_from_objects(
    'a', 'sum_if_even', $args1, \@objs, 'florins'
  );
  is_deeply(\@avus1, \@expected1,
            'AVUs from objects, with arguments and units');

  my $args2 = [];
  my @expected2 = (
    {attribute => 'b', value => 42},
    {attribute => 'b', value => 42},
    {attribute => 'b', value => 42}, );
  my @avus2 = $irods->make_avus_from_objects('b', 'answer', $args2, \@objs);
  is_deeply(\@avus2, \@expected2,
            'AVUs from objects, without arguments and units');

  my $args3 = [[1,2], [2,3]];
  dies_ok {$irods->make_avus_from_objects('c', 'sum_if_even', $args3, \@objs)}
    'Dies with incorrect number of argument ArrayRefs';

  my $args4 = [1, 2, 3];
  dies_ok {$irods->make_avus_from_objects('d', 'sum_if_even', $args4, \@objs)}
    'Dies with arguments which are not ArrayRefs';

}


sub remote_duplicate_avus : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  # Single AVU (no-op)
  is_deeply([$irods->remove_duplicate_avus($irods->make_avu('a', 'b'))],
            [$irods->make_avu('a', 'b')]);

  # Only a duplicate
  is_deeply([$irods->remove_duplicate_avus($irods->make_avu('a', 'b'),
                                           $irods->make_avu('a', 'b'))],
            [$irods->make_avu('a', 'b')]);

  # Single plus a duplicate, illustrating sorting
  is_deeply([$irods->remove_duplicate_avus($irods->make_avu('c', 'd'),
                                           $irods->make_avu('a', 'b'),
                                           $irods->make_avu('a', 'b'))],
            [$irods->make_avu('a', 'b'),
             $irods->make_avu('c', 'd')]);

  # Multiple duplicates, illustrating sorting
  is_deeply([$irods->remove_duplicate_avus($irods->make_avu('c', 'd'),
                                           $irods->make_avu('a', 'b'),
                                           $irods->make_avu('c', 'e'),
                                           $irods->make_avu('a', 'c'),
                                           $irods->make_avu('a', 'c'),
                                           $irods->make_avu('a', 'b'),
                                           $irods->make_avu('a', 'c'),
                                           $irods->make_avu('c', 'd'),
                                           $irods->make_avu('c', 'e'))],
            [$irods->make_avu('a', 'b'),
             $irods->make_avu('a', 'c'),
             $irods->make_avu('c', 'd'),
             $irods->make_avu('c', 'e')]);
}

sub avus_equal : Test(9) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  ok($irods->avus_equal({attribute => 'a', value => 'v', units => 'u'},
                        {attribute => 'a', value => 'v', units => 'u'}),
     'AVUs =');

  ok(!$irods->avus_equal({attribute => 'a', value => 'v', units => 'u'},
                         {attribute => 'b', value => 'v', units => 'u'}),
     'AVUs != on a');

  ok(!$irods->avus_equal({attribute => 'a', value => 'v', units => 'u'},
                         {attribute => 'a', value => 'w', units => 'u'}),
     'AVUs != on v');

  ok(!$irods->avus_equal({attribute => 'a', value => 'v', units => 'u'},
                         {attribute => 'a', value => 'v', units => 'v'}),
     'AVUs != on u');

  ok(!$irods->avus_equal({attribute => 'a', value => 'v'},
                         {attribute => 'a', value => 'v', units => 'u'}),
     'AVU != AV');

  ok(!$irods->avus_equal({attribute => 'a', value => 'v', units => 'u'},
                         {attribute => 'a', value => 'v'}),
     'AV != AVU');

  ok($irods->avus_equal({attribute => 'a', value => 'v'},
                        {attribute => 'a', value => 'v'}),
     'AVs =');

  ok(!$irods->avus_equal({attribute => 'a', value => 'v'},
                         {attribute => 'b', value => 'v'}),
     'AVs != on a');

  ok(!$irods->avus_equal({attribute => 'a', value => 'v'},
                         {attribute => 'a', value => 'w'}),
     'AVs != on u');
}

1;
