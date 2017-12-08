package WTSI::NPG::iRODS::PublisherFactoryTest;

use strict;
use warnings;
use Log::Log4perl;

use Test::More;
use Test::Exception;

use base qw[WTSI::NPG::iRODS::TestRabbitMQ];

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::NPG::iRODS::PublisherFactory;

sub require : Test(1) {
    require_ok('WTSI::NPG::iRODS::PublisherFactory');
}

sub make_publishers : Test(3) {

    my $factory = WTSI::NPG::iRODS::PublisherFactory->new();

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0);
    my %args = ( 'irods' =>  $irods );

    my $publisher;
    local %ENV = %ENV;
    $ENV{NPG_RMQ_CONFIG} ||= './etc/rmq_test_config.json';
    $publisher = $factory->make_publisher(%args);
    isa_ok($publisher, 'WTSI::NPG::iRODS::PublisherWithReporting');
    $ENV{NPG_RMQ_CONFIG} = 0;
    $publisher = $factory->make_publisher(%args);
    isa_ok($publisher, 'WTSI::NPG::iRODS::Publisher');
    # ensure we have an instance of the parent class, not the subclass
    ok(!($publisher->isa('WTSI::NPG::iRODS::PublisherWithReporting')),
       'Factory does not return a PublisherWithReporting');

}
