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
        diag('Omitting test class: Either TEST_RABBITMQ is set to ',
             'false; or TEST_RABBITMQ is not set, and TEST_AUTHOR ',
             'is false or not set');
        $self->SKIP_CLASS($skip_msg);
    } else {
	# modules needed for RabbitMQ tests
        require WTSI::NPG::iRODS::PublisherFactory;
        require WTSI::NPG::iRODS::PublisherWithReporting;
	require WTSI::NPG::iRODSMQTest;
	require WTSI::NPG::RabbitMQ::TestCommunicator;
    }
    return $self->SUPER::runtests;
}


sub rmq_subscriber_args {
    my ($self, $channel, $conf, $test_host) = @_;
    my $args = {
        hostname             => $test_host, # global variable
        rmq_config_path      => $conf,      # global variable
        channel              => $channel,
    };
    return $args;
}

sub rmq_test_collection_message {
    my ($self, $message, $method, $expected_body, $irods) = @_;
    # 10 tests in total
    return $self->_test_message($message, $method, $expected_body, $irods, 0);
}

sub rmq_test_object_message {
    my ($self, $message, $method, $expected_body, $irods) = @_;
    # 11 tests in total
    return $self->_test_message($message, $method, $expected_body, $irods, 1);
}

sub _test_message {
  # General-purpose method to test RabbitMQ messages.
  #
  # TODO remove duplication with method in ReporterTest.pm
  #
  # Arguments:
  # - [ArrayRef] RabbitMQ message, consisting of body and headers
  # - [Str] Method name
  # - [HashRef] Expected body of message.
  # - [WTSI::NPG::iRODS] iRODS object, used for sorting AVUs
  # - [Bool] Flag to indicate a data object (as opposed to a collection)
  #
  # Tests performed:
  # - Exact values of method, user, and irods_user headers
  # - Format of timestamp header
  # - Presence of file type header (value may be an empty string)
  # - Exact values of collection, data object and AVUs (if any) in body

  my ($self, $message, $method, $expected_body, $irods, $is_data_object) = @_;

  my $log = Log::Log4perl::get_logger();

  my $expected_headers = 5; # timestamp, user, irods_user, type, method
  my $expected_body_keys_total = scalar keys(%{$expected_body});

  my $total_tests = 10;
  if ($is_data_object) { $total_tests++; }

  my $skip = not defined($message);
  if ($skip) {
    $log->logwarn('Unexpectedly got an undefined message from RabbitMQ; ',
          'skipping subsequent tests on content of the message');
  }
 SKIP: {
   # If message undefined, skip tests on content to improve readability
   # Distinct from option to skip all RabbitMQ tests; see TestRabbitMQ.pm
    skip "RabbitMQ message not defined", $total_tests if $skip;
    my ($body, $headers) = @{$message};

    # expected number of header/body fields
    ok(scalar keys(%{$headers}) == $expected_headers,
       'Found '.$expected_headers.' header key/value pairs.');
    ok(scalar keys(%{$body}) == $expected_body_keys_total,
       'Found '.$expected_body_keys_total.' body key/value pairs.');

    # check content of headers
    ok($headers->{'method'} eq $method, "Header method name is $method");
    my $time = $headers->{'timestamp'};
    ok($time =~ /\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}/msx,
       "Header timestamp '$time' is in correct format");
    my $user = $ENV{'USER'};
    ok($headers->{'user'} eq $user, "Header user name is $user");
    ok($headers->{'irods_user'} eq $user, "Header iRODS user name is $user");
    ok(defined $headers->{'type'},
       "Header file type is defined (may be an empty string)");

    # check content of body
    ok($body->{'collection'} eq $expected_body->{'collection'},
       'Collection matches expected value');
    if ($is_data_object) {
      ok($body->{'data_object'} eq $expected_body->{'data_object'},
     'Data object matches expected value');
    }
    # sort AVUs to ensure consistent order for comparison
    my @avus = $irods->sort_avus(@{$body->{'avus'}});
    my @expected_avus = $irods->sort_avus(@{$expected_body->{'avus'}});
    is_deeply(\@avus, \@expected_avus, 'AVUs match expected values');
    # sort ACL to ensure consistent order for comparison
    my @acl = $irods->sort_acl(@{$body->{'acl'}});
    my @expected_acl = $irods->sort_acl(@{$expected_body->{'acl'}});
    is_deeply(\@acl, \@expected_acl, 'ACL matches expected value');


  }
}


1;
