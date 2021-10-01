package WTSI::NPG::iRODS::Test;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More;

# Run full tests (requiring a test iRODS server)

sub runtests {
  my ($self) = @_;

  my $default_irods_env = 'IRODS_ENVIRONMENT_FILE';
  my $test_irods_env = "WTSI_NPG_iRODS_Test_$default_irods_env";
  defined $ENV{$test_irods_env} or
    die "iRODS test environment variable $test_irods_env was not set";

  my %env_copy = %ENV;

  # Ensure that the iRODS connection details are set to the test environment
  $env_copy{$default_irods_env} = $ENV{$test_irods_env};

  {
    local %ENV = %env_copy;
    return $self->SUPER::runtests;
  }
}

# If any test methods fail to complete, count all their remaining
# tests as failures.
sub fail_if_returned_early {
  return 1;
}

sub have_admin_rights {
  my ($self, $have_rights) = @_;

  if (defined $have_rights) {
    $self->{_have_admin_rights} = $have_rights;
  }
  else {
    $self->{_have_admin_rights} = system("iadmin lu > /dev/null") == 0;
  }

  return $self->{_have_admin_rights};
}

sub add_irods_groups {
  my ($self, $irods, @groups) = @_;

  my @groups_added;
  foreach my $group (@groups) {
    if ($self->have_admin_rights and not $irods->group_exists($group)) {
      push @groups_added, $irods->add_group($group);
    }
  }

  return @groups_added;
}

sub remove_irods_groups {
  my ($self, $irods, @groups) = @_;

  my @groups_removed;
  foreach my $group (@groups) {
    if ($self->have_admin_rights and $irods->group_exists($group)) {
      push @groups_removed, $irods->remove_group($group);
    }
  }

  return @groups_removed;
}

1;
