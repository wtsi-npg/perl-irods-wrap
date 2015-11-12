package WTSI::NPG::iRODS::Test;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More;

# Run full tests (requiring a test iRODS server) only if TEST_AUTHOR
# is true. If full tests are run, require that irodsEnvFile be set.
sub runtests {
  my ($self) = @_;

  my $env_file = $ENV{'WTSI_NPG_iRODS_Test_irodsEnvFile'} || q{};
  if (not $env_file) {
    if ($ENV{TEST_AUTHOR}) {
      die 'Environment variable WTSI_NPG_iRODS_Test_irodsEnvFile was not set';
    }
    else {
      $self->SKIP_CLASS('TEST_AUTHOR environment variable is false');
    }
  }

  {
    local $ENV{'irodsEnvFile'} = $env_file;
    return $self->SUPER::runtests;
  }
}

1;
