package WTSI::NPG::iRODS::PerformanceTest;

use strict;
use warnings;
use Benchmark qw(:all);
use English qw(-no_match_vars);
use File::Temp qw(tempdir);
use List::AllUtils qw(any);

use base qw(WTSI::NPG::iRODS::Test);
use Test::More;

use WTSI::NPG::iRODS;

my $irods_tmp_coll;

my $fixture_counter = 0;
my $pid = $PID;

WTSI::NPG::iRODS::PerformanceTest->SKIP_CLASS
  ($ENV{TEST_PERFORMANCE} ? 0 :
   'TEST_PERFORMANCE is not set; skipping performance tests');

my @test_hosting_zones = qw(tempZone testZone Sanger1-dev);
my $can_test = 0;

sub is_test_zone {
  my ($irods) = @_;

  my $zone = $irods->find_zone_name($irods->working_collection);

  return any { $_ eq $zone } @test_hosting_zones;
}

sub setup_test : Test(setup) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  $irods_tmp_coll =
    $irods->add_collection("CollectionTest.$pid.$fixture_counter");
  $fixture_counter++;
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

  $irods->remove_collection($irods_tmp_coll);
}

# These tests are intended to warn of any massive regressions in
# performance. Typically, the operations being tested should be much
# faster than the thresholds that trigger a failure.

sub collection_operations : Test(3) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

 SKIP: {
     if (not is_test_zone($irods)) {
       skip "Skipping test because this is not a iRODS test zone", 3;
     }

     my $num_objs = 1000;

     my $tmpdir = tempdir(CLEANUP => 1);
     foreach my $i (1 .. $num_objs) {
       my $file = "$tmpdir/$i";
       open my $out, '>', $file or
         die "Failed to open $file for writing: $!\n";
       print $out $i, "\n";
       close $out;
     }

     my $coll = $irods->put_collection($tmpdir, $irods_tmp_coll);
     foreach my $attr (qw(a b c)) {
       foreach my $value (qw(x y)) {
         my $units = $value eq 'x' ? 'cm' : undef;
         $irods->add_collection_avu($coll, $attr, $value, $units);
       }
     }

     # See 'perldoc Benchmark'
     my @timestr_args = ('noc');
     my $n = 5;

     my $is_coll = timethis($n, sub { $irods->is_collection($coll) });
     ok($is_coll->real < 5, "list_collection < 5s");
     diag 'is_collection: ', timestr($is_coll, @timestr_args);

     my $list_coll = timethis($n, sub { $irods->list_collection($coll) });
     ok($list_coll->real < 10, "list_collection < 10s");
     diag 'list_collection: ', timestr($list_coll, @timestr_args);

     my $coll_checksums =
       timethis($n, sub { $irods->collection_checksums($coll) });
     ok($coll_checksums->real < 180, "collection_checksums < 180s");
     diag 'collection_checksums: ', timestr($coll_checksums, @timestr_args);
   }
}

sub object_operations : Test(6) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);

 SKIP: {
    if (not is_test_zone($irods)) {
      skip "Skipping test because this is not a iRODS test zone", 3;
    }

    my $num_objs = 100;

    my $tmpdir = tempdir(CLEANUP => 1);
    foreach my $i (1 .. $num_objs) {
      my $file = "$tmpdir/$i";
      open my $out, '>', $file or die "Failed to open $file for writing: $!\n";
      print $out $i, "\n";
      close $out;
    }

    my $coll = $irods->put_collection($tmpdir, $irods_tmp_coll);
    my ($objs, $colls) = $irods->list_collection($coll);
    foreach my $obj (@$objs) {
      foreach my $attr (qw(a b c)) {
        foreach my $value (qw(x y)) {
          my $units = $value eq 'x' ? 'cm' : undef;
          $irods->add_object_avu($obj, $attr, $value, $units);
        }
      }
    }

    my @timestr_args = ('noc');
    my $n = 5;

    my $is_obj =
      timethis($n, sub {
                 foreach my $i (1 .. $num_objs) {
                   $irods->is_object("$coll/$i");
                 }
               });
    ok($is_obj->real < 10, "is_object $num_objs < 10s");
    diag 'is_object:', timestr($is_obj, @timestr_args);

    my $list_obj =
      timethis($n, sub {
                 foreach my $i (1 .. $num_objs) {
                   $irods->list_object("$coll/$i");
                 }
               });
    ok($list_obj->real < 10, "list_object $num_objs < 10s");
    diag 'list_object:', timestr($list_obj, @timestr_args);

    my $get_obj_perms =
      timethis($n, sub {
                 foreach my $i (1 .. $num_objs) {
                   $irods->get_object_permissions("$coll/$i");
                 }
               });
    ok($get_obj_perms->real < 30, "get_object_permissions $num_objs < 30s");
    diag 'get_object_permissions: ', timestr($get_obj_perms, @timestr_args);

    my $get_obj_groups =
      timethis($n, sub {
                 foreach my $i (1 .. $num_objs) {
                   $irods->get_object_groups("$coll/$i");
                 }
             });
    ok($get_obj_groups->real < 30, "get_object_groups $num_objs < 30s");
    diag 'get_object_groups: ', timestr($get_obj_groups, @timestr_args);

    my $get_obj_meta =
      timethis($n, sub {
                 foreach my $i (1 .. $num_objs) {
                   $irods->get_object_meta("$coll/$i");
                 }
               });
    ok($get_obj_meta->real < 30, "get_object_meta $num_objs < 30s");
    diag 'get_object_meta: ', timestr($get_obj_meta, @timestr_args);

    my $find_objs_meta =
      timethis($n, sub {
                 foreach my $i (1 .. $num_objs) {
                   $irods->find_objects_by_meta($coll, ['a' => 'x']);
                 }
               });
    ok($find_objs_meta->real < 60, "find_objects_by_meta $num_objs < 60s");
    diag 'find_objects_by_meta: ', timestr($find_objs_meta, @timestr_args);
  }
}

1;
