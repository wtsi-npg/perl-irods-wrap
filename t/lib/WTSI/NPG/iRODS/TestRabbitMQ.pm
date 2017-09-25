package WTSI::NPG::iRODS::TestRabbitMQ;

use strict;
use warnings;

use base qw(WTSI::NPG::iRODS::Test);

use Test::More;

# Run full tests (requiring a test RabbitMQ server) only if specified by
# environment variables:
#
# - If TEST_RABBITMQ is set to a false value, skip RabbitMQ tests.
#
# - If TEST_RABBITMQ is not set, fall back to TEST_AUTHOR. Run RabbitMQ
# tests if TEST_AUTHOR is true; skip tests if it is false or undefined.
#
# Typical use case: TEST_AUTHOR is true, to enable tests using iRODS.
# Then default behaviour is to run RabbitMQ tests as well, unless explicitly
# cancelled by setting TEST_RABBITMQ to false.

sub runtests {
    my ($self) = @_;
    my $run_tests;
    my $skip_msg; # message to print if skipping tests
    if (! defined $ENV{TEST_RABBITMQ}) {
        $run_tests = $ENV{TEST_AUTHOR};
        $skip_msg = 'TEST_RABBITMQ environment variable not set; '.
            'TEST_AUTHOR false or not set'
    } else {
        $run_tests = $ENV{TEST_RABBITMQ};
        $skip_msg = 'TEST_RABBITMQ environment variable is false';
    }
    if (! $run_tests) {
        diag('Omitting RabbitMQ tests: Either TEST_RABBITMQ is set to ',
             'false; or TEST_RABBITMQ is not set, and TEST_AUTHOR ',
             'is false or not set');
        $self->SKIP_CLASS($skip_msg);
    }
    return $self->SUPER::runtests;
}

1;
