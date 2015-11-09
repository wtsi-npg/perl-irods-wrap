package WTSI::NPG::iRODS::Test;

use strict;
use warnings;

use base qw(Test::Class);
use Test::More;

sub runtests {
  my ($self) = @_;

  my $env_file = $ENV{'WTSI_NPG_iRODS_Test_irodsEnvFile'};

  if ($env_file) {
    local $ENV{'irodsEnvFile'} = $env_file;
    return $self->SUPER::runtests;
  }
  else {
    die 'Environment variable WTSI_NPG_iRODS_Test_irodsEnvFile was not set';
  }
}

1;

