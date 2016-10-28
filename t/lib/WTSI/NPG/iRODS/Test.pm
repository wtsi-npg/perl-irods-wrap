package WTSI::NPG::iRODS::Test;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More;

# Run full tests (requiring a test iRODS server) only if TEST_AUTHOR
# is true. If full tests are run, require that both irodsEnvFile
# IRODS_ENVIRONMENT_FILE and be set. This is for safety because we do
# not know which of 3.x or 4.x clients will be first on the PATH. The
# unused variable may be set to a dummy value.

sub runtests {
  my ($self) = @_;

  my %env_copy = %ENV;

  # iRODS 3.* and iRODS 4.* have different env vars for configuration
  foreach my $file (qw(irodsEnvFile IRODS_ENVIRONMENT_FILE)) {
    my $env_file = $ENV{"WTSI_NPG_iRODS_Test_$file"} || q[];

    # Ensure that the iRODS connection details are a nonsense value if
    # they are not set explicitly via WTSI_NPG_iRODS_Test_*
    $env_copy{$file} = $env_file || 'DUMMY_VALUE';

    if (not $env_file) {
      if ($ENV{TEST_AUTHOR}) {
        die "Environment variable WTSI_NPG_iRODS_Test_$file was not set";
      }
      else {
        $self->SKIP_CLASS('TEST_AUTHOR environment variable is false');
      }
    }
  }

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

1;
