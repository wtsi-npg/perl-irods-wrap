package WTSI::NPG::iRODS::TestRabbitMQ;

use strict;
use warnings;

use base qw(WTSI::NPG::iRODS::Test);

# Run full tests (requiring a test RabbitMQ server) only if
# environment variable TEST_RABBITMQ is true.
#
# If TEST_RABBITMQ is true, test configuration is determined by the
# NPG_RMQ_HOST and NPG_RMQ_CONFIG variables if these are set, or default
# values otherwise.
#
# RabbitMQ checks are run in addition to iRODS checks from
# WTSI::NPG::iRODS::Test.

sub runtests {
    my ($self) = @_;
    if (! $ENV{TEST_RABBITMQ}) {
	$self->SKIP_CLASS('TEST_RABBITMQ environment variable is false');
    }
    return $self->SUPER::runtests;
}

1;
