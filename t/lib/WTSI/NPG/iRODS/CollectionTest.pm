package WTSI::NPG::iRODS::CollectionTest;

use strict;
use warnings;
use English qw(-no_match_vars);
use File::Spec;
use List::AllUtils qw(all any none);
use Log::Log4perl;

use base qw(WTSI::NPG::iRODS::Test);
use Test::More;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::NPG::iRODS::Collection;

my $fixture_counter = 0;
my $data_path = './t/data/path';
my $irods_tmp_coll;

my $pid = $PID;

my $have_admin_rights =
  system(qq{$WTSI::NPG::iRODS::IADMIN lu >/dev/null 2>&1}) == 0;

# Prefix for test iRODS data access groups
my $group_prefix = 'ss_';
# Groups to be added to the test iRODS
my @irods_groups = map { $group_prefix . $_ } (10, 100);
# Groups added to the test iRODS in fixture setup
my @groups_added;

sub setup_test : Test(setup) {
  my ($self) = @_;

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("CollectionTest.$pid.$fixture_counter");
  $fixture_counter++;
  $irods->put_collection($data_path, $irods_tmp_coll);

  foreach my $attr (qw(a b c)) {
    foreach my $value (qw(x y)) {
      my $test_coll = "$irods_tmp_coll/path/test_dir";
      my $units = $value eq 'x' ? 'cm' : undef;

      $irods->add_collection_avu($test_coll, $attr, $value, $units);
    }
  }

  @groups_added = $self->add_irods_groups($irods, @irods_groups);
}

sub teardown_test : Test(teardown) {
  my ($self) = @_;

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  $irods->remove_collection($irods_tmp_coll);
  $self->remove_irods_groups($irods, @groups_added);
}

sub require : Test(1) {
  require_ok('WTSI::NPG::iRODS::Collection');
}

sub constructor : Test(6) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  new_ok('WTSI::NPG::iRODS::Collection', [$irods, '.']);

  new_ok('WTSI::NPG::iRODS::Collection', [$irods, './']);

  new_ok('WTSI::NPG::iRODS::Collection', [$irods, '/']);

  new_ok('WTSI::NPG::iRODS::Collection', [$irods, '/foo']);

  new_ok('WTSI::NPG::iRODS::Collection', [$irods, '/foo/bar']);

  dies_ok {
    WTSI::NPG::iRODS::Collection->new(irods        => $irods,
                                      collection   => '/foo',
                                      spurious_arg => 'spurious_value')
    };
}

sub collection : Test(6) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  my $path1 = WTSI::NPG::iRODS::Collection->new($irods, '.');
  ok($path1->has_collection, 'Has collection 1');
  is($path1->collection, '.');

  my $path2 = WTSI::NPG::iRODS::Collection->new($irods, '/');
  ok($path2->has_collection, 'Has collection 2');
  is($path2->collection, '/');

  my $path3 = WTSI::NPG::iRODS::Collection->new($irods, '/foo/');
  ok($path3->has_collection, 'Has collection 3');
  is($path3->collection, '/foo');
}

sub is_present : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $coll_path = "$irods_tmp_coll/path/test_dir";
  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);

  ok($coll->is_present, 'Collection is present');

  ok(!WTSI::NPG::iRODS::Collection->new
     ($irods, "/no_such_object_collection")->is_present,
     'Collection is not present');
}

sub absolute : Test(4) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $wc = $irods->working_collection;

  my $coll1 = WTSI::NPG::iRODS::Collection->new($irods, ".");
  is($coll1->absolute->str, $wc, 'Absolute collection from relative 1');

  my $coll2 = WTSI::NPG::iRODS::Collection->new($irods, "./");
  is($coll2->absolute->str, $wc, 'Absolute collection from relative 2');

  my $coll3 = WTSI::NPG::iRODS::Collection->new($irods, "/");
  is($coll3->absolute->str, '/', 'Absolute collection from relative 3');

  my $coll4 = WTSI::NPG::iRODS::Collection->new($irods, "foo");
  is($coll4->absolute->str, "$wc/foo", 'Absolute collection from relative 4');
}

sub metadata : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $coll_path = "$irods_tmp_coll/path/test_dir";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'b', value => 'y'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'}];

  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);
  is_deeply($coll->metadata, $expected_meta,
            'Collection metadata loaded') or diag explain $coll->metadata;
}

sub get_avu : Test(3) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $coll_path = "$irods_tmp_coll/path/test_dir";
  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);

  my $avu = $coll->get_avu('a', 'x');
  is_deeply($avu, {attribute => 'a', value => 'x', units => 'cm'},
            'Matched one AVU 1');

  ok(!$coll->get_avu('does_not_exist', 'does_not_exist'),
     'Handles missing AVU');

  dies_ok { $coll_path->get_avu('a') }
    "Expected to fail getting ambiguous AVU";
}

sub add_avu : Test(5) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $coll_path = "$irods_tmp_coll/path/test_dir";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'a', value => 'z'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'b', value => 'y'},
                       {attribute => 'b', value => 'z'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'},
                       {attribute => 'c', value => 'z'}];

  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);
  ok($coll->add_avu('a' => 'z'));
  ok($coll->add_avu('b' => 'z'));
  ok($coll->add_avu('c' => 'z'));

  my $meta = $coll->metadata;
  is_deeply($meta, $expected_meta,
            'Collection metadata AVUs added 1') or diag explain $meta;

  # Flush the cache to re-read from iRODS
  $coll->clear_metadata;

  $meta = $coll->metadata;
  is_deeply($meta, $expected_meta,
            'Collection metadata AVUs added 2') or diag explain $meta;
}

sub remove_avu : Test(5) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $coll_path = "$irods_tmp_coll/path/test_dir";
  my $expected_meta = [{attribute => 'a', value => 'y'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'x', units => 'cm'}];

  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);
  ok($coll->remove_avu('a' => 'x', 'cm'));
  ok($coll->remove_avu('b' => 'y'));
  ok($coll->remove_avu('c' => 'y'));

  my $meta = $coll->metadata;
  is_deeply($meta, $expected_meta,
            'Collection metadata AVUs removed 1') or diag explain $meta;

  # Flush the cache to re-read from iRODS
  $coll->clear_metadata;

  $meta = $coll->metadata;
  is_deeply($meta, $expected_meta,
            'Collection metadata AVUs removed 2') or diag explain $meta;
}

sub str : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $coll_path = "$irods_tmp_coll/path/test_dir";
  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);

  is($coll->str, $coll_path, 'Collection string');
}

sub get_contents : Test(8) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $coll_path = "$irods_tmp_coll/path/test_dir/contents";

  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);

  my ($objs, $colls) = $coll->get_contents;

  my @obj_paths  = map { $_->str } @$objs;
  my @coll_paths = map { $_->str } @$colls;

  my $expected_objs  = [];
  my $expected_colls = [$coll_path,
                        "$coll_path/a",
                        "$coll_path/b",
                        "$coll_path/c"];

  is_deeply(\@obj_paths, $expected_objs, 'Object contents')
    or diag explain \@obj_paths;

  is_deeply(\@coll_paths, $expected_colls, 'Collection contents')
    or diag explain \@coll_paths;

  my ($objs_r, $colls_r) = $coll->get_contents('RECURSE');
  my @obj_paths_r  = map { $_->str } @$objs_r;
  my @coll_paths_r = map { $_->str } @$colls_r;

  my $expected_objs_r  = ["$coll_path/a/10.txt",
                          "$coll_path/a/x/1.txt",
                          "$coll_path/b/20.txt",
                          "$coll_path/b/y/2.txt",
                          "$coll_path/c/30.txt",
                          "$coll_path/c/z/3.txt"];
  my $expected_colls_r = [$coll_path,
                          "$coll_path/a",
                          "$coll_path/a/x",
                          "$coll_path/b",
                          "$coll_path/b/y",
                          "$coll_path/c",
                          "$coll_path/c/z"];

  is_deeply(\@obj_paths_r, $expected_objs_r, 'Object recursive contents')
    or diag explain \@obj_paths_r;

  is_deeply(\@coll_paths_r, $expected_colls_r, 'Collection recursive contents')
    or diag explain \@coll_paths_r;

  my $expected_checksums = ['31d30eea8d0968d6458e0ad0027c9f80',
                            'b026324c6904b2a9cb4b88d6d61c81d1',
                            'dbbf8220893d497d403bb9cdf49db7a4',
                            '26ab0db90d72e28ad0ba1e22ee510510',
                            'd5b4c7d9b06b60a7846c4529834c9812',
                            '6d7fce9fee471194aa8b5b6e47267f03'];

  # Checksums read individually, on demand
  my @checksums_r = map { $_->checksum } @$objs_r;
  is_deeply(\@checksums_r, $expected_checksums, 'Object checksums') or
      diag explain \@checksums_r;

  my ($objs_rc, $colls_rc) = $coll->get_contents('RECURSE', 'CHECKSUM');
  my @obj_paths_rc  = map { $_->str } @$objs_r;
  my @coll_paths_rc = map { $_->str } @$colls_r;

  is_deeply(\@obj_paths_rc, $expected_objs_r, 'Object recursive contents')
    or diag explain \@obj_paths_rc;

  is_deeply(\@coll_paths_rc, $expected_colls_r, 'Collection recursive contents')
    or diag explain \@coll_paths_rc;

  # Checksums fetched as a batch
  my @checksums_rc = map { $_->checksum } @$objs_rc;
  is_deeply(\@checksums_r, $expected_checksums, 'Object checksums') or
      diag explain \@checksums_rc;
}

sub get_permissions : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $coll_path = "$irods_tmp_coll/path/test_dir";
  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);

  my $perms = all { exists $_->{owner} &&
                    exists $_->{level} }
    $coll->get_permissions;
  ok($perms, 'Permissions obtained');
}

sub set_permissions : Test(9) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $coll_path = "$irods_tmp_coll/path/test_dir";
  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);

  my $r0 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $coll->get_permissions;
  ok($r0, 'No public read access');

  ok($coll->set_permissions('read', 'public'),
     'Set permission using an implicit zone');

  my $r1 = any { exists $_->{owner} && $_->{owner} eq 'public' &&
                 exists $_->{level} && $_->{level} eq 'read' }
    $coll->get_permissions;
  ok($r1, 'Added public read access');

  ok($coll->set_permissions(undef, 'public'));

  my $r2 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $coll->get_permissions;
  ok($r2, 'Removed public read access');

  my $zone = $irods->find_zone_name($irods_tmp_coll);
  ok($coll->set_permissions('read', "public#$zone"),
     'Set permission using an explicit zone');

  dies_ok { $coll->set_permissions('bogus_permission', 'public') }
    'Fails to set bogus permission';

  dies_ok { $coll->set_permissions('read', 'bogus_group') }
    'Fails to set permission for bogus group';

 SKIP: {
    my $version = $irods->installed_baton_version;
    my ($dotted_version, $commits) = $version =~ m{^(\d+[.]\d+[.]\d+)(\S*)$}msx;

    skip "baton $version is < 0.16.3", 1 unless
      version->parse($dotted_version) > version->parse('0.16.2');

    dies_ok { $coll->set_permissions('read', 'public#no_such_zone') }
      'Fails to set permission using a non-existent zone';
  }
}

sub get_groups : Test(7) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
  my $coll_path = "$irods_tmp_coll/path/test_dir";
  my $coll = WTSI::NPG::iRODS::Collection->new($irods, $coll_path);

 SKIP: {
    if (not $irods->group_exists('ss_0')) {
      skip "Skipping test requiring the test group ss_0", 6;
    }

    ok($irods->set_collection_permissions('read', 'public', $coll_path));
    ok($irods->set_collection_permissions('read', 'ss_0',   $coll_path));
    ok($irods->set_collection_permissions('read', 'ss_10',  $coll_path));

    my $expected_all = ['ss_0', 'ss_10'];
    my @found_all  = $coll->get_groups;
    is_deeply(\@found_all, $expected_all, 'Expected all groups')
      or diag explain \@found_all;

    my $expected_read = ['ss_0', 'ss_10'];
    my @found_read = $coll->get_groups('read');
    is_deeply(\@found_read, $expected_read, 'Expected read groups')
      or diag explain \@found_read;

    $irods->group_filter(sub {
                           my ($owner) = @_;
                           if ($owner =~ m{^(public|ss_)}) {
                             return 1;
                           }
                         });
    my $expected_filter = ['public', 'ss_0', 'ss_10'];
    my @found_filter  = $coll->get_groups;
    is_deeply(\@found_filter, $expected_filter, 'Expected filtered groups')
      or diag explain \@found_filter;
  }

  my $expected_own = [];
  my @found_own  = $coll->get_groups('own');
  is_deeply(\@found_own, $expected_own, 'Expected own groups')
    or diag explain \@found_own;
}

1;
