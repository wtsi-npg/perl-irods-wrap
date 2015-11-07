
use strict;
use warnings;

local $ENV{'irodsEnvFile'} = $ENV{'WTSI_NPG_iRODS_Test_irodsEnvFile'};
BEGIN{
  $ENV{'irodsEnvFile'} = $ENV{'WTSI_NPG_iRODS_Test_irodsEnvFile'};
}BEGIN{
  use WTSI::NPG::DriRODSTest;
}

if(not $ENV{'irodsEnvFile'}){
  WTSI::NPG::DriRODSTest->SKIP_CLASS('No test iRODS enviroment found from environment variable "WTSI_NPG_iRODS_Test_irodsEnvFile"');
}

Test::Class->runtests;
