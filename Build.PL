
use utf8;

use strict;
use warnings;

# These paths are propagated to @INC in the build script. The 't/lib'
# directory is added because the Test::Class tests are there.
use lib qw(. t/lib);

use Module::Build;
use Build;

my $build = Build->new
  (module_name        => 'WTSI::NPG::iRODS',
   dist_author        => ['David Jackson <dj3@sanger.ac.uk>',
                          'Keith James <kdj@sanger.ac.uk>'],
   dist_abstract      => 'Perl iRods Client Wrapper Library',
   dist_version       => Build->git_tag,
   license            => 'gpl',
   configure_requires => {
                          'Module::Build' => 0.42
                         },
   test_requires      => {
                          'TAP::Harness'    => '>= 3.30',
                          'Test::Class'     => '>= 0.41',
                          'Test::More'      => '>= 0.98',
                          'Test::Exception' => '>= 0.32',
                         },
   requires =>           {
                          'Carp'            => '>= 1.20',
                          'Cache::Cache'    => '>= 1.06',
                          'Encode'          => '>= 2.42',
                          'File::Which'     => '>= 1.09',
                          'IPC::Run'        => '>= 0.92',
                          'JSON'            => '>= 2.61',
                          'Log::Log4perl'   => '>= 1.42',
                          'List::AllUtils'  => '>= 0.07',
                          'Moose'           => '>= 2.1',
                          'Readonly'        => '>= 1.04',
                          'Set::Scalar'     => '>= 1.26',
                          'URI'             => '>= 1.60'
                         });

$build->create_build_script;
