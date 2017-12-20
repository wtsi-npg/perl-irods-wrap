package WTSI::NPG::iRODS::PublisherFactoryTest;

use strict;
use warnings;
use Log::Log4perl;

use Test::More;
use Test::Exception;

use base qw[WTSI::NPG::iRODS::TestRabbitMQ];

Log::Log4perl::init('./etc/log4perl_tests.conf');

sub require : Test(1) {
    require_ok('WTSI::NPG::iRODS::PublisherFactory');
}

sub make_publishers : Test(6) {

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0);

    my $factory0 = WTSI::NPG::iRODS::PublisherFactory->new(
        enable_rmq         => 0,
        irods              => $irods,
    );
    my $publisher0 = $factory0->make_publisher();
    isa_ok($publisher0, 'WTSI::NPG::iRODS::Publisher');
    # ensure we have an instance of the parent class, not the subclass
    ok(!($publisher0->isa('WTSI::NPG::iRODS::PublisherWithReporting')),
       'Factory does not return a PublisherWithReporting');

    my $factory1 = WTSI::NPG::iRODS::PublisherFactory->new(
        channel            => 42,
        enable_rmq         => 1,
        exchange           => 'foo',
        irods              => $irods,
        routing_key_prefix => 'bar',
    );
    my $publisher1 = $factory1->make_publisher();
    isa_ok($publisher1, 'WTSI::NPG::iRODS::PublisherWithReporting');
    is($publisher1->channel, 42, 'channel attribute is correct');
    is($publisher1->exchange, 'foo', 'exchange attribute is correct');
    is($publisher1->routing_key_prefix, 'bar',
       'routing_key_prefix attribute is correct');

}
