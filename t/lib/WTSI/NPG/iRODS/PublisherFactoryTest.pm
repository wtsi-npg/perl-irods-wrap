package WTSI::NPG::iRODS::PublisherFactoryTest;

use strict;
use warnings;
use Log::Log4perl;

use Test::More;
use Test::Exception;

use base qw[WTSI::NPG::iRODS::TestRabbitMQ];

Log::Log4perl::init('./etc/log4perl_tests.conf');

# dummy class to consume the PublisherFactory Role

{
    package WTSI::NPG::iRODS::MockPublisherBuilder;

    use Moose;

    with 'WTSI::NPG::iRODS::PublisherFactory';

}

require WTSI::NPG::iRODS::MockPublisherBuilder;

sub make_publishers : Test(7) {

    my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                      strict_baton_version => 0);
    my %args = ( 'irods' =>  $irods );
    my %bad_args = (
        irods              => $irods,
        exchange           => 'foo',
        routing_key_prefix => 'bar',
    );

    my $factory0 = WTSI::NPG::iRODS::MockPublisherBuilder->new(
        enable_rmq => 0
    );
    my $publisher0 = $factory0->make_publisher(%args);
    isa_ok($publisher0, 'WTSI::NPG::iRODS::Publisher');
    # ensure we have an instance of the parent class, not the subclass
    ok(!($publisher0->isa('WTSI::NPG::iRODS::PublisherWithReporting')),
       'Factory does not return a PublisherWithReporting');
    dies_ok { $factory0->make_publisher(%bad_args) }
        'Publisher creation dies with incorrect arguments';

    my $factory1 = WTSI::NPG::iRODS::MockPublisherBuilder->new(
        enable_rmq         => 1,
        exchange           => 'foo',
        routing_key_prefix => 'bar',
    );
    my $publisher1 = $factory1->make_publisher(%args);
    is($publisher1->exchange, 'foo', 'exchange attribute is correct');
    is($publisher1->routing_key_prefix, 'bar',
       'routing_key_prefix attribute is correct');
    isa_ok($publisher1, 'WTSI::NPG::iRODS::PublisherWithReporting');
    dies_ok { $factory1->make_publisher(%bad_args) }
        'PublisherWithReporting creation dies with incorrect arguments';

}
