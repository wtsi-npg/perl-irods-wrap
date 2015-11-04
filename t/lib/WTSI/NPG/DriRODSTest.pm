package WTSI::NPG::DriRODSTest;

use strict;
use warnings;
use English qw(-no_match_vars);
use File::Temp;
use List::AllUtils qw(none);
use Log::Log4perl;

use base qw(Test::Class);
use Test::More tests => 54;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::DriRODS'); }

use WTSI::NPG::DriRODS;
use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::DataObject;
use WTSI::NPG::iRODS::Types qw(:all);

my $pid = $PID;
my $cwc = WTSI::NPG::iRODS->new(strict_baton_version => 0)->working_collection;

my $fixture_counter = 0;
my $data_path = './t/irods';
my $irods_tmp_coll;

my $have_admin_rights =
  system(qq{$WTSI::NPG::iRODS::IADMIN lu >/dev/null 2>&1}) == 0;

# Prefix for test iRODS data access groups
my $group_prefix = 'ss_';
# Groups to be added to the test iRODS
my @irods_groups = map { $group_prefix . $_ } (0);
# Groups added to the test iRODS in fixture setup
my @groups_added;
# Enable group tests
my $group_tests_enabled = 0;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  $irods_tmp_coll = $irods->add_collection("iRODSTest.$pid.$fixture_counter");
  $fixture_counter++;
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

  my $group_count = 0;
  foreach my $group (@irods_groups) {
    if ($irods->group_exists($group)) {
      $group_count++;
    }
    else {
      if ($have_admin_rights) {
        push @groups_added, $irods->add_group($group);
        $group_count++;
      }
    }
  }

  if ($group_count == scalar @irods_groups) {
    $group_tests_enabled = 1;
  }
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  $irods->working_collection($cwc);
  $irods->remove_collection($irods_tmp_coll);
}

sub add_group : Test(3) {
  my $drirods = WTSI::NPG::DriRODS->new(strict_baton_version => 0);

  my $test_group = "test_group." . $PID;

 SKIP: {
    if (not $have_admin_rights and $group_tests_enabled) {
      skip 'iRODS group tests were not enabled with admin rights', 3;
    }

    ok(!$drirods->group_exists($test_group), 'Test group not present');
    ok($drirods->add_group($test_group), 'Dry run add_group');
    ok(!$drirods->group_exists($test_group), 'Test group not added');
  }
}

sub remove_group : Test(3) {
  my $drirods = WTSI::NPG::DriRODS->new(strict_baton_version => 0);

  my $test_group = "ss_0";

 SKIP: {
    if (not $have_admin_rights and $group_tests_enabled) {
      skip 'iRODS group tests were not enabled with admin rights', 3;
    }

    ok($drirods->group_exists($test_group), 'Test group present');
    ok($drirods->remove_group($test_group), 'Dry run remove_group');
    ok($drirods->group_exists($test_group), 'Test group not removed');
  }
}

sub set_group_access : Test(3) {
  my $drirods = WTSI::NPG::DriRODS->new(strict_baton_version => 0);
  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";

  my $zone = $drirods->find_zone_name($irods_tmp_coll);
  my $r0 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{zone}  && $_->{zone}  eq $zone    &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $drirods->get_object_permissions($lorem_object);
  ok($r0, 'No public read access before');

  ok($drirods->set_group_access('read', 'public', $lorem_object),
    'Dry run set_group_access');

  my $r1 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{zone}  && $_->{zone}  eq $zone    &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $drirods->get_object_permissions($lorem_object);
  ok($r0, 'No public read access after');
}

sub set_object_permissions : Test(3) {
  my $drirods = WTSI::NPG::DriRODS->new(strict_baton_version => 0);
  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";

  my $zone = $drirods->find_zone_name($irods_tmp_coll);
  my $r0 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{zone}  && $_->{zone}  eq $zone    &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $drirods->get_object_permissions($lorem_object);
  ok($r0, 'No public read access before');

  ok($drirods->set_object_permissions('read', 'public', $lorem_object),
    'Dry run set_object_permissions');

  my $r1 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{zone}  && $_->{zone}  eq $zone    &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $drirods->get_object_permissions($lorem_object);
  ok($r0, 'No public read access after');
}

sub set_collection_permissions : Test(3) {
  my $drirods = WTSI::NPG::DriRODS->new(strict_baton_version => 0);
  my $coll = "$irods_tmp_coll/irods";

  my $zone = $drirods->find_zone_name($irods_tmp_coll);
  my $r0 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{zone}  && $_->{zone}  eq $zone    &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $drirods->get_collection_permissions($coll);
  ok($r0, 'No public read access before');

  ok($drirods->set_collection_permissions('read', 'public', $coll),
    'Dry run set_object_permissions');

  my $r1 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{zone}  && $_->{zone}  eq $zone    &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $drirods->get_collection_permissions($coll);
  ok($r0, 'No public read access after');
}

sub add_collection : Test(2) {
  my $drirods = WTSI::NPG::DriRODS->new(strict_baton_version => 0);

  # Deliberate spaces in names
  my $coll = "$irods_tmp_coll/add_ _collection";
  ok($drirods->add_collection($coll), 'Dry run add_collection');
  ok(!$drirods->list_collection($coll), 'Collection not added');
}

sub put_collection : Test(2) {
  my $drirods = WTSI::NPG::DriRODS->new(strict_baton_version => 0);

  my $dir = File::Spec->catdir($data_path, 'test');
  my $target = "$irods_tmp_coll";

  ok($drirods->put_collection($dir, $target), 'Dry run put_collection');
  ok(!$drirods->list_collection("$target/test"), 'No collection put');
}

sub move_collection : Test(3) {
  my $drirods = WTSI::NPG::DriRODS->new(strict_baton_version => 0);

  my $coll_to_move = "$irods_tmp_coll/irods";
  my $coll_moved = "$irods_tmp_coll/irods_moved";

  ok($drirods->move_collection($coll_to_move, $coll_moved),
     'Dry run move_collection');

  ok($drirods->list_collection($coll_to_move), 'Collection was not moved 1');
  ok(!$drirods->list_collection($coll_moved), 'Collection was not moved 2');
}

sub remove_collection : Test(2) {
  my $drirods = WTSI::NPG::DriRODS->new(strict_baton_version => 0);

  my $coll = "$irods_tmp_coll/irods";
  ok($drirods->remove_collection($coll),'Dry run remove_collection');
  ok($drirods->list_collection($coll), 'Collection was not removed');
}

sub add_collection_avu : Test(10) {
  my $drirods = WTSI::NPG::DriRODS->new(strict_baton_version => 0);

  my $num_attrs = 8;
  my %meta = map { 'cattr' . $_ => 'cval' . $_ } 0 .. $num_attrs;

  my $test_coll = $irods_tmp_coll;
  foreach my $attr (keys %meta) {
    ok($drirods->add_collection_avu($test_coll, $attr, $meta{$attr}),
       'Dry run add_collection_avu');
  }

  my @observed_meta = $drirods->get_collection_meta($test_coll);
  is_deeply(\@observed_meta, [], 'Collection metadata not added') or
    diag explain \@observed_meta;
}

sub remove_collection_avu : Test(3) {
  my $drirods = WTSI::NPG::DriRODS->new(strict_baton_version => 0);

  my $coll = "$irods_tmp_coll/irods";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'b', value => 'y'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'}];

  ok($drirods->remove_collection_avu($coll, 'b', 'x', 'cm'),
    'Dry run remove_collection_avu 1');
  ok($drirods->remove_collection_avu($coll, 'b', 'y'),
     'Dry run remove_collection_avu 2');

  my @observed_meta = $drirods->get_collection_meta($coll);
  is_deeply(\@observed_meta, $expected_meta,
            'Metadata not removed') or diag explain \@observed_meta;
}

sub add_object : Test(2) {
  my $drirods = WTSI::NPG::DriRODS->new(strict_baton_version => 0);

  my $lorem_file = "$data_path/lorem.txt";
  my $lorem_object = "$irods_tmp_coll/lorem_added.txt";
  ok($drirods->add_object($lorem_file, $lorem_object), 'Dry run add_object');
  ok(!$drirods->list_object($lorem_object), 'Data object not added');
}

sub replace_object : Test(3) {
  my $drirods = WTSI::NPG::DriRODS->new(strict_baton_version => 0);

  my $tmp = File::Temp->new;
  my $empty_file = $tmp->filename;
  my $to_replace = "$irods_tmp_coll/irods/lorem.txt";

  my $checksum_before = $drirods->checksum($to_replace);

  ok($drirods->replace_object($empty_file, $to_replace),
     'Dry run replace_object');
  is($drirods->checksum($to_replace), $checksum_before,
     'Object was not replaced');
}

sub copy_object : Test(3) {
  my $drirods = WTSI::NPG::DriRODS->new(strict_baton_version => 0);

  my $to_copy = "$irods_tmp_coll/irods/lorem.txt";
  my $copy    = "$irods_tmp_coll/irods/lorem_copy.txt";

  ok($drirods->copy_object($to_copy, $copy), 'Dry run copy_object');
  is($drirods->list_object($to_copy), $to_copy, 'Data object still present');
  ok(!$drirods->list_object($copy), 'Data object not copied');
}

sub move_object : Test(3) {
  my $drirods = WTSI::NPG::DriRODS->new(strict_baton_version => 0);

  my $to_move = "$irods_tmp_coll/irods/lorem.txt";
  my $moved   = "$irods_tmp_coll/irods/lorem_moved.txt";

  ok($drirods->move_object($to_move, $moved), 'Dry run move_object');
  is($drirods->list_object($to_move), $to_move, 'Data object still present');
  ok(!$drirods->list_object($moved), 'Data object not moved');
}

sub remove_object : Test(2) {
  my $drirods = WTSI::NPG::DriRODS->new(strict_baton_version => 0);

  my $to_remove = "$irods_tmp_coll/irods/lorem.txt";

  ok($drirods->remove_object($to_remove), 'Dry run remove_object');
  is($drirods->list_object($to_remove), $to_remove, 'Data object not removed');
}

sub remove_object_avu : Test(3) {
  my $drirods = WTSI::NPG::DriRODS->new(strict_baton_version => 0);

  my $lorem_object = "$irods_tmp_coll/irods/lorem.txt";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'b', value => 'y'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'}];

  ok($drirods->remove_object_avu($lorem_object, 'b', 'x', 'cm'),
     'Dry run remove_object_avu 1');
  ok($drirods->remove_object_avu($lorem_object, 'b', 'y'),
     'Dry run remove_object_avu 2');

  my @observed_meta = $drirods->get_object_meta($lorem_object);
  is_deeply(\@observed_meta, $expected_meta,
            'Metadata not removed') or diag explain \@observed_meta;
}

1;
