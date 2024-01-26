package WTSI::NPG::iRODS::GroupAdminTest;

use strict;
use warnings;
use Log::Log4perl;

use base qw(WTSI::NPG::iRODS::Test);
use Test::More;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::NPG::iRODS;
use WTSI::NPG::iRODS::GroupAdmin;

my $have_admin_rights =
  system(qq{$WTSI::NPG::iRODS::IADMIN lu >/dev/null 2>&1}) == 0;

# Prefix for test iRODS data access groups
my $group_prefix = 'ss_';

# Groups to be added to the test iRODS
my @irods_groups = map { $group_prefix . $_ } (0 .. 5);
my @irods_users = qw(user_foo user_bar);

my $test_irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                       strict_baton_version => 0);

sub setup_test : Test(setup) {
  my ($self) = @_;

  $self->have_admin_rights($have_admin_rights);


  $self->add_irods_groups($test_irods, @irods_groups);
  if ($self->have_admin_rights) {
    foreach my $user (@irods_users) {
      system(qq{$WTSI::NPG::iRODS::IADMIN mkuser '$user' rodsuser}) == 0 or
        warn "Failed to add user test '$user'";
    }
  }
}

sub teardown_test : Test(teardown) {
  my ($self) = @_;

  $self->remove_irods_groups($test_irods, @irods_groups, q(ss_newempty));

  if ($self->have_admin_rights) {
    foreach my $user (@irods_users) {
      system(qq{$WTSI::NPG::iRODS::IADMIN rmuser '$user'}) == 0 or
        warn "Failed to clean up test user '$user'";
    }
  }
}

sub constructor : Test(2) {
  new_ok('WTSI::NPG::iRODS::GroupAdmin');

  dies_ok {
    local %ENV = %ENV;
    $ENV{PATH} = q("/bin");

    WTSI::NPG::iRODS::GroupAdmin->new;
  }
}

sub lg : Test(5) {
  my ($self) = @_;

  my $iga = WTSI::NPG::iRODS::GroupAdmin->new;
  ok($iga->lg('public'), 'Found public group');

 SKIP: {
    if (not $self->have_admin_rights) {
      skip 'No admin rights to create test groups', 2;
    }

    # Ignore the rodsadmin group as it is not present in iRODS >4.3.0
    my @observed_groups = sort grep { $_ ne 'rodsadmin' } $iga->lg;
    my @expected_groups = sort ('public', @irods_groups);
    is_deeply(\@observed_groups, \@expected_groups, 'Expected groups found') or
      diag explain \@observed_groups;

    cmp_ok(scalar $iga->lg('ss_0'), '==', 0,
           'Zero member group');
  }

  throws_ok { $iga->lg('ss_000') } qr/does not exist/sm,
    'Non-existent group throw';

  throws_ok { $iga->lg(q()); } qr/empty string/sm,
    'Empty string group throw';
}

sub ensure_group_exists: Test(6) {
  my ($self) = @_;

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
 SKIP: {
    if (not $self->have_admin_rights) {
      skip 'No admin rights to create test groups', 5;
    }

    my $zone = $irods->find_zone_name($irods->working_collection);
    my $user = $irods->get_irods_user;

    my $iga = WTSI::NPG::iRODS::GroupAdmin->new;

    throws_ok { $iga->lg('ss_newempty') } qr/does not exist/sm,
           'Non-existent group throw for ss_newempty';

    my$created=0;
    lives_ok {
      $created=$iga->ensure_group_exists('ss_newempty');
    } 'Create new group ss_newempty';
    ok($created, "Reports that group has been created");

    my @observed_members = sort $iga->lg('ss_newempty');
    my @expected_members = ("$user#$zone");
    is_deeply(\@observed_members, \@expected_members,
              'Has expected admin user automatically added') or
                diag explain \@observed_members;

    $created=1;
    lives_ok {
      $created=$iga->ensure_group_exists('ss_newempty');
    } 'Safe to run on existing group';

    ok(!$created, "Reports that group has not been created");
  }
}

sub set_group_membership : Test(5) {
  my ($self) = @_;

  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0);
 SKIP: {
    if (not $self->have_admin_rights) {
      skip 'No admin rights to create test groups', 5;
    }

    my $zone = $irods->find_zone_name($irods->working_collection);
    my $user = $irods->get_irods_user;

    my $iga = WTSI::NPG::iRODS::GroupAdmin->new;

    cmp_ok(scalar $iga->lg('ss_0'), '==', 0,
           'Has zero members initially');

    my @members = map { $_ . "#$zone"} @irods_users;

    lives_ok {
      $iga->set_group_membership('ss_0', @members);
    } 'Add 2 members to group';

    my @observed_members = sort $iga->lg('ss_0');
    my @expected_members = sort ("$user#$zone", @members);
    is_deeply(\@observed_members, \@expected_members,
              'Has expected members (as admin user automatically added)') or
                diag explain \@observed_members;

    lives_ok {
      $iga->set_group_membership('ss_0');
    } 'Empty group membership';

    is_deeply([$iga->lg('ss_0')], ["$user#$zone"],
              'Has 1 member (admin user remains)');
  }
}

1;
