
use utf8;

use strict;
use warnings;

# These paths are propagated to @INC in the build script. The 't/lib'
# directory is added because the Test::Class tests are there.
use lib qw(. t/lib);

use Module::Build;
use Build;

my $build = Build->new
  (module_name     => 'WTSI::NPG::iRODS',
   dist_author     => ['David Jackson <dj3@sanger.ac.uk>',
                       'Keith James <kdj@sanger.ac.uk>'],
   dist_abstract   => 'Perl iRods Client Wrapper Library',
   dist_version    => Build->git_tag,
   license         => 'gpl',
   requires => {
                'Carp'               => 0,
                'Cache::MemoryCache' => 0,
                'Encode'             => 0,
                'File::Which'        => 0,
                'IPC::Run'           => 0,
                'JSON'               => 0,
                'Log::Log4perl'      => 0,
                'List::AllUtils'     => 0,
                'Moose'              => 0,
                'Readonly'           => 0,
                'Set::Scalar'        => 0,
                'TAP::Harness'       => 0,
                'Test::Class'        => 0,
                'Test::More'         => 0,
                'Test::Exception'    => 0,
                'URI'                => 0
               });

$build->create_build_script;
