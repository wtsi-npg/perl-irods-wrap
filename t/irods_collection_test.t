
use strict;
use warnings;

use WTSI::NPG::iRODS::CollectionTest;

local $ENV{'irodsEnvFile'} = $ENV{'WTSI_NPG_iRODS_Test_irodsEnvFile'};
if(not $ENV{'irodsEnvFile'}){
  WTSI::NPG::iRODS::CollectionTest->SKIP_CLASS('No test iRODS enviroment found from environment variable "WTSI_NPG_iRODS_Test_irodsEnvFile"');
}


Test::Class->runtests;
