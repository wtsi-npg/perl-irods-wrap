
use strict;
use warnings;

# These paths are propagated to @INC in the build script. The 't/lib'
# directory is added because the Test::Class tests are there.
use lib qw(lib t/lib);

use WTSI::DNAP::Utilities::Build;

my $build = WTSI::DNAP::Utilities::Build->new
  (module_name        => 'WTSI::NPG::iRODS',
   dist_author        => ['David Jackson <dj3@sanger.ac.uk>',
                          'Keith James <kdj@sanger.ac.uk>'],
   dist_abstract      => 'Perl iRODS Client Wrapper Library',
   dist_version       => WTSI::DNAP::Utilities::Build->report_version,
   license            => 'gpl',
   configure_requires => {
                          'Module::Build'         => '>= 0.42'
                         },
   test_requires      => {
                          'TAP::Harness'          => '>= 3.30',
                          'Test::Class'           => '>= 0.41',
                          'Test::More'            => '>= 0.98',
                          'Test::Exception'       => '>= 0.32',
                         },
   requires =>           {
                          'Carp'                  => '>= 1.20',
                          'Encode'                => '>= 2.42',
                          'File::Which'           => '>= 1.09',
                          'DateTime'              => '>= 1.18',
                          'IO::Pty'               => '>= 1.10',
                          'IPC::Run'              => '>= 0.92',
                          'Log::Log4perl'         => '>= 1.46',
                          'List::AllUtils'        => '>= 0.09',
                          'Moose'                 => '>= 2.1',
                          'MooseX::Types'         => '>= 0.45',
                          'Readonly'              => '>= 1.04',
                          'Set::Scalar'           => '>= 1.29',
                          'Try::Tiny'             => '>= 0.22',
                          'URI'                   => '>= 1.67',
                          'WTSI::DNAP::Utilities' => '>= 0.4.2'
                         },
   recommends =>         {
                          'Net::LDAP'             => '0'
                         });

$build->create_build_script;
