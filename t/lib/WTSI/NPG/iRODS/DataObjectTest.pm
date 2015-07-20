
package WTSI::NPG::iRODS::DataObjectTest;

use strict;
use warnings;
use File::Spec;
use List::AllUtils qw(all any none);
use Log::Log4perl;

use base qw(Test::Class);
use Test::More tests => 90;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

BEGIN { use_ok('WTSI::NPG::iRODS::DataObject'); }

use WTSI::NPG::iRODS::DataObject;

my $fixture_counter = 0;
my $data_path = './t/irods_path_test';
my $irods_tmp_coll;

my $pid = $$;

my @groups_added;

my $have_admin_rights =
  system(qq{$WTSI::NPG::iRODS::IADMIN lu 2>&1 /dev/null}) == 0;

sub make_fixture : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("DataObjectTest.$pid.$fixture_counter");
  $fixture_counter++;
  $irods->put_collection($data_path, $irods_tmp_coll);

  my $i = 0;
  foreach my $attr (qw(a b c)) {
    foreach my $value (qw(x y)) {
      my $test_coll = "$irods_tmp_coll/irods_path_test/test_dir";
      my $test_obj = File::Spec->join($test_coll, 'test_file.txt');
      my $units = $value eq 'x' ? 'cm' : undef;

      $irods->add_object_avu($test_obj, $attr, $value, $units);
    }
  }

  if ($have_admin_rights) {
    foreach my $group (qw(ss_0 ss_10)) {
      unless ($irods->group_exists($group)) {
        push @groups_added, $irods->add_group($group);
      }
    }
  }
}

sub teardown : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  $irods->remove_collection($irods_tmp_coll);

  if ($have_admin_rights) {
    foreach my $group (@groups_added) {
      if ($irods->group_exists($group)) {
        $irods->remove_group($group);
      }
    }
  }
}

sub require : Test(1) {
  require_ok('WTSI::NPG::iRODS::DataObject');
}

sub constructor : Test(3) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  new_ok('WTSI::NPG::iRODS::DataObject', [irods       => $irods,
                                          collection  => '/foo',
                                          data_object => 'bar.txt']);

  new_ok('WTSI::NPG::iRODS::DataObject', [irods       => $irods,
                                          data_object => 'bar.txt']);

  new_ok('WTSI::NPG::iRODS::DataObject', [irods       => $irods,
                                          data_object => './bar.txt']);
}

sub data_object : Test(12) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);

  my $path1 = WTSI::NPG::iRODS::DataObject->new($irods, '/foo/bar.txt');
  ok($path1->has_collection, 'Has collection 1');
  ok($path1->has_data_object, 'Has data object 1');
  is($path1->collection, '/foo');
  is($path1->data_object, 'bar.txt');

  my $path2 = WTSI::NPG::iRODS::DataObject->new($irods, 'bar.txt');
  ok($path2->has_collection, 'Has collection 2');
  ok($path2->has_data_object, 'Has data object 2');
  is($path2->collection, '.');
  is($path2->data_object, 'bar.txt');

  my $path3 = WTSI::NPG::iRODS::DataObject->new($irods, './bar.txt');
  ok($path3->has_collection, 'Has collection 3');
  ok($path3->has_data_object, 'Has data object 3');
  is($path3->collection, '.');
  is($path3->data_object, 'bar.txt');
}

sub is_present : Test(2) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);
  ok($obj->is_present, 'Object is present');

  ok(!WTSI::NPG::iRODS::DataObject->new
     ($irods, "no_such_object.txt")->is_present);
}

sub absolute : Test(3) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);
  my $wc = $irods->working_collection;

  my $obj1 = WTSI::NPG::iRODS::DataObject->new($irods, "./foo.txt");
  is($obj1->absolute->str, "$wc/foo.txt", 'Absolute path from relative 1');

  my $obj2 = WTSI::NPG::iRODS::DataObject->new($irods, "foo.txt");
  is($obj2->absolute->str, "$wc/foo.txt", 'Absolute path from relative 2');

  my $obj3 = WTSI::NPG::iRODS::DataObject->new($irods, "/foo.txt");
  is($obj3->absolute->str, '/foo.txt', 'Absolute path from relative 3');
}

sub metadata : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'b', value => 'y'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'}];

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);
  is_deeply($obj->metadata, $expected_meta,
            'DataObject metadata loaded') or diag explain $obj->metadata;
}

sub get_avu : Test(3) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);

  my $avu = $obj->get_avu('a', 'x');
  is_deeply($avu, {attribute => 'a', value => 'x', units => 'cm'},
            'Matched one AVU 1');

  ok(!$obj->get_avu('does_not_exist', 'does_not_exist'), 'Handles missing AVU');

  dies_ok { $obj_path->get_avu('a') }
    "Expected to fail getting ambiguous AVU";
}

sub add_avu : Test(5) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $expected_meta = [{attribute => 'a', value => 'x', units => 'cm'},
                       {attribute => 'a', value => 'y'},
                       {attribute => 'a', value => 'z'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'b', value => 'y'},
                       {attribute => 'b', value => 'z'},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'},
                       {attribute => 'c', value => 'z'}];

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);
  ok($obj->add_avu('a' => 'z'));
  ok($obj->add_avu('b' => 'z'));
  ok($obj->add_avu('c' => 'z'));

  my $meta = $obj->metadata;
  is_deeply($meta, $expected_meta,
            'DataObject metadata AVUs added 1') or diag explain $meta;

  # Flush the cache to re-read from iRODS
  $obj->clear_metadata;

  $meta = $obj->metadata;
  is_deeply($meta, $expected_meta,
            'DataObject metadata AVUs added 2') or diag explain $meta;
}

sub remove_avu : Test(5) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $expected_meta = [{attribute => 'a', value => 'y'},
                       {attribute => 'b', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'x', units => 'cm'}];

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);
  ok($obj->remove_avu('a' => 'x', 'cm'));
  ok($obj->remove_avu('b' => 'y'));
  ok($obj->remove_avu('c' => 'y'));

  my $meta = $obj->metadata;
  is_deeply($meta, $expected_meta,
            'DataObject metadata AVUs removed 1') or diag explain $meta;

  # Flush the cache to re-read from iRODS
  $obj->clear_metadata;

  $meta = $obj->metadata;
  is_deeply($meta, $expected_meta,
            'DataObject metadata AVUs removed 2') or diag explain $meta;
}

sub supersede_avus : Test(10) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $history_timestamp1 = DateTime->now;

  # Perform one update of 'a' and 'b'
  my $history_value1a = sprintf "[%s] x,y", $history_timestamp1->iso8601;
  my $history_value1b = sprintf "[%s] x,y", $history_timestamp1->iso8601;
  my $expected_meta1 = [{attribute => 'a', value => 'new_a'},
                        {attribute => 'a_history', value => $history_value1a},
                        {attribute => 'b', value => 'new_b', units => 'km'},
                        {attribute => 'b_history', value => $history_value1b},
                        {attribute => 'c', value => 'x', units => 'cm'},
                        {attribute => 'c', value => 'y'}];

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);

  ok($obj->supersede_avus('a' => 'new_a', undef, $history_timestamp1));
  ok($obj->supersede_avus('b' => 'new_b', 'km', $history_timestamp1));

  my $meta1 = $obj->metadata;
  is_deeply($meta1, $expected_meta1,
            'DataObject metadata AVUs superseded 1') or diag explain $meta1;

  # Flush the cache to re-read from iRODS
  $obj->clear_metadata;

  $meta1 = $obj->metadata;
  is_deeply($meta1, $expected_meta1,
            'DataObject metadata AVUs superseded 1, flushed cache')
    or diag explain $meta1;

  # Perform another update of 'a'
  my $history_timestamp2 = DateTime->now->add(seconds => 10);
  my $history_value2a = sprintf "[%s] new_a", $history_timestamp2->iso8601;
  my $expected_meta2 = [{attribute => 'a', value => 'x'},
                        {attribute => 'a_history', value => $history_value1a},
                        {attribute => 'a_history', value => $history_value2a},
                        {attribute => 'b', value => 'new_b', units => 'km'},
                        {attribute => 'b_history', value => $history_value1b},
                        {attribute => 'c', value => 'x', units => 'cm'},
                        {attribute => 'c', value => 'y'}];
  ok($obj->supersede_avus('a' => 'x', undef, $history_timestamp2));

  my $meta2 = $obj->metadata;
  is_deeply($meta2, $expected_meta2,
            'DataObject metadata AVUs superseded 2') or diag explain $meta2;

  # Flush the cache to re-read from iRODS
  $obj->clear_metadata;

  is_deeply($meta2, $expected_meta2,
            'DataObject metadata AVUs superseded 2, flushed cache')
    or diag explain $meta2;

  # "Supersede" an AVU that is not present; this should be equivalent
  # to the simple addition of a new AVU and should not create any
  # history.
  ok($obj->supersede_avus('zzzzzz' => 'new_zzzzzz', undef,
                          $history_timestamp2));

 my $expected_meta3 = [{attribute => 'a', value => 'x'},
                       {attribute => 'a_history', value => $history_value1a},
                       {attribute => 'a_history', value => $history_value2a},
                       {attribute => 'b', value => 'new_b', units => 'km'},
                       {attribute => 'b_history', value => $history_value1b},
                       {attribute => 'c', value => 'x', units => 'cm'},
                       {attribute => 'c', value => 'y'},
                       {attribute => 'zzzzzz', value => 'new_zzzzzz'}];

  my $meta3 = $obj->metadata;
  is_deeply($meta3, $expected_meta3,
            'DataObject metadata AVUs superseded 3') or diag explain $meta3;

  # Flush the cache to re-read from iRODS
  $obj->clear_metadata;

  is_deeply($meta3, $expected_meta3,
            'DataObject metadata AVUs superseded 3, flushed cache')
    or diag explain $meta3;
}

sub supersede_multivalue_avus : Test(7) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $history_timestamp1 = DateTime->now;

  # Perform one update of 'a' and 'b'
  my $history_value1a = sprintf "[%s] x,y", $history_timestamp1->iso8601;
  my $expected_meta1 = [{attribute => 'a', value => 'new_a1'},
                        {attribute => 'a', value => 'new_a2'},
                        {attribute => 'a', value => 'new_a3'},
                        {attribute => 'a_history', value => $history_value1a},
                        {attribute => 'b', value => 'x', units => 'cm'},
                        {attribute => 'b', value => 'y'},
                        {attribute => 'c', value => 'x', units => 'cm'},
                        {attribute => 'c', value => 'y'}];

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);

  ok($obj->supersede_multivalue_avus('a' => ['new_a1', 'new_a2', 'new_a3'],
                                     undef, $history_timestamp1));

  my $meta1 = $obj->metadata;
  is_deeply($meta1, $expected_meta1,
            'DataObject metadata multivalue AVUs superseded 1')
    or diag explain $meta1;

  # Flush the cache to re-read from iRODS
  $obj->clear_metadata;

  $meta1 = $obj->metadata;
  is_deeply($meta1, $expected_meta1,
            'DataObject metadata multivalue AVUs superseded 1, flushed cache')
    or diag explain $meta1;

  # Perform another update of 'a'
  my $history_timestamp2 = DateTime->now->add(seconds => 10);
  my $history_value2a = sprintf "[%s] new_a1,new_a2,new_a3",
    $history_timestamp2->iso8601;
  my $expected_meta2 = [{attribute => 'a', value => 'new_a4'},
                        {attribute => 'a', value => 'new_a5'},
                        {attribute => 'a', value => 'new_a6'},
                        {attribute => 'a_history', value => $history_value1a},
                        {attribute => 'a_history', value => $history_value2a},
                        {attribute => 'b', value => 'x', units => 'cm'},
                        {attribute => 'b', value => 'y'},
                        {attribute => 'c', value => 'x', units => 'cm'},
                        {attribute => 'c', value => 'y'}];
  ok($obj->supersede_multivalue_avus('a' => ['new_a4', 'new_a5', 'new_a6'],
                                     undef, $history_timestamp2));

  my $meta2 = $obj->metadata;
  is_deeply($meta2, $expected_meta2,
            'DataObject metadata multivalue AVUs superseded 2')
    or diag explain $meta2;

  # Flush the cache to re-read from iRODS
  $obj->clear_metadata;

  is_deeply($meta2, $expected_meta2,
            'DataObject metadata multivalue AVUs superseded 2, flushed cache')
    or diag explain $meta2;
}

sub abandon_avus : Test(8) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $history_timestamp1 = DateTime->now;

  # Abandon AVUs with attribute 'a' and 'b'
  my $history_value1a = sprintf "[%s] x,y", $history_timestamp1->iso8601;
  my $history_value1b = sprintf "[%s] x,y", $history_timestamp1->iso8601;
  my $expected_meta1 = [{attribute => 'a_history', value => $history_value1a},
                        {attribute => 'b_history', value => $history_value1b},
                        {attribute => 'c', value => 'x', units => 'cm'},
                        {attribute => 'c', value => 'y'}];

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);

  ok($obj->abandon_avus('a', $history_timestamp1));
  ok($obj->abandon_avus('b', $history_timestamp1));

  my $meta1 = $obj->metadata;
  is_deeply($meta1, $expected_meta1,
            'DataObject metadata AVUs abandoned 1') or diag explain $meta1;

  # Flush the cache to re-read from iRODS
  $obj->clear_metadata;

  $meta1 = $obj->metadata;
  is_deeply($meta1, $expected_meta1,
            'DataObject metadata AVUs abandoned 1, flushed cache')
    or diag explain $meta1;

  # Abandon AVUs with attribute 'c'
  my $history_timestamp2 = DateTime->now->add(seconds => 10);
  my $history_value2a = sprintf "[%s] x,y", $history_timestamp2->iso8601;
  my $expected_meta2 = [{attribute => 'a_history', value => $history_value1a},
                        {attribute => 'b_history', value => $history_value1b},
                        {attribute => 'c_history', value => $history_value2a}];

  ok($obj->abandon_avus('c', $history_timestamp2));

  my $meta2 = $obj->metadata;
  is_deeply($meta2, $expected_meta2,
            'DataObject metadata AVUs abandoned 2') or diag explain $meta2;

  # Flush the cache to re-read from iRODS
  $obj->clear_metadata;

  is_deeply($meta2, $expected_meta2,
            'DataObject metadata AVUs abandoned 2, flushed cache')
    or diag explain $meta2;

  # "Abandon" AVUs that are not present
  ok($obj->abandon_avus('zzzzzz'), 'Abandoned an AVU that is not present');
}

sub str : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);
  is($obj->str, $obj_path, 'DataObject string');
}

sub checksum : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";

  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);
  is($obj->checksum, "d41d8cd98f00b204e9800998ecf8427e",
     'Has correct checksum');
}

sub get_permissions : Test(1) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);

  my $perms = all { exists $_->{owner} &&
                    exists $_->{level} }
    $obj->get_permissions;
  ok($perms, 'Permissions obtained');
}

sub set_permissions : Test(7) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);

  # Begin
  my $r0 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $obj->get_permissions;
  ok($r0, 'No public read access');

  # Set public read
  ok($obj->set_permissions('read', 'public'));

  my $r1 = any { exists $_->{owner} && $_->{owner} eq 'public' &&
                 exists $_->{level} && $_->{level} eq 'read' }
    $obj->get_permissions;
  ok($r1, 'Added public read access');

  # Remove public read
  ok($obj->set_permissions(undef, 'public'));

  my $r2 = none { exists $_->{owner} && $_->{owner} eq 'public' &&
                  exists $_->{level} && $_->{level} eq 'read' }
    $obj->get_permissions;
  ok($r2, 'Removed public read access');

  dies_ok { $obj->set_permissions('bogus_permission', 'public') }
    'Fails to set bogus permission';

  dies_ok { $obj->set_permissions('read', 'bogus_group') }
    'Fails to set permission for bogus group';
}

sub get_groups : Test(6) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);

 SKIP: {
    if (not $irods->group_exists('ss_0')) {
      skip "Skipping test requiring the test group ss_0", 5;
    }

    ok($irods->set_object_permissions('read', 'public', $obj_path));
    ok($irods->set_object_permissions('read', 'ss_0',   $obj_path));
    ok($irods->set_object_permissions('read', 'ss_10',  $obj_path));

    my $expected_all = ['ss_0', 'ss_10'];
    my @found_all  = $obj->get_groups;
    is_deeply(\@found_all, $expected_all, 'Expected all groups')
      or diag explain \@found_all;

    my $expected_read = ['ss_0', 'ss_10'];
    my @found_read = $obj->get_groups('read');
    is_deeply(\@found_read, $expected_read, 'Expected read groups')
      or diag explain \@found_read;
  }

  my $expected_own = [];
  my @found_own  = $obj->get_groups('own');
  is_deeply(\@found_own, $expected_own, 'Expected own groups')
    or diag explain \@found_own;
}

sub update_group_permissions : Test(13) {
  my $irods = WTSI::NPG::iRODS->new(strict_baton_version => 0);
  my $obj_path = "$irods_tmp_coll/irods_path_test/test_dir/test_file.txt";
  my $obj = WTSI::NPG::iRODS::DataObject->new($irods, $obj_path);

 SKIP: {
    if (not $irods->group_exists('ss_0')) {
      skip "Skipping test requiring the test group ss_0", 13;
    }

    # Begin
    my $r0 = none { exists $_->{owner} && $_->{owner} eq 'ss_0' &&
                    exists $_->{level} && $_->{level} eq 'read' }
      $obj->get_permissions;
    ok($r0, 'No ss_0 read access');

    # Add a study 0 AVU and use it to update (add) permissions
    ok($obj->add_avu('study_id', '0'));
    ok($obj->update_group_permissions);

    my $r1 = any { exists $_->{owner} && $_->{owner} eq 'ss_0' &&
                   exists $_->{level} && $_->{level} eq 'read' }
      $obj->get_permissions;
    ok($r1, 'Added ss_0 read access');

    # Remove the study 0 AVU and use it to update (remove) permissions
    ok($obj->remove_avu('study_id', '0'));
    ok($obj->update_group_permissions);

    my $r2 = none { exists $_->{owner} && $_->{owner} eq 'ss_0' &&
                    exists $_->{level} && $_->{level} eq 'read' }
      $obj->get_permissions;
    ok($r2, 'Removed ss_0 read access');

    # Add a study 0 AVU and use it to update (add) permissions
    # in the presence of anAVU that will infer a non-existent group
    ok($obj->add_avu('study_id', '0'));
    ok($obj->add_avu('study_id', 'no_such_group'));
    ok($obj->update_group_permissions);

    my $r3 = any { exists $_->{owner} && $_->{owner} eq 'ss_0' &&
                   exists $_->{level} && $_->{level} eq 'read' }
      $obj->get_permissions;
    ok($r3, 'Restored ss_0 read access');

    # The bogus study AVU should trigger an exception in strict groups
    # mode
    dies_ok {
      my $strict_groups = 1;
      ok($obj->update_group_permissions($strict_groups));
    } 'An unknown iRODS group causes failure';
  }
}

1;
