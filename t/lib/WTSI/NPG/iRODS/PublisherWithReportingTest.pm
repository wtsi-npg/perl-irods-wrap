package WTSI::NPG::iRODS::PublisherWithReportingTest;

use strict;
use warnings;

use Carp;
use English qw[-no_match_vars];
use File::Copy::Recursive qw[dircopy];
use File::Temp;
use JSON;
use Log::Log4perl;
use Test::Exception;
use Test::More;

use base qw[WTSI::NPG::iRODS::TestRabbitMQ];

Log::Log4perl::init('./etc/log4perl_tests.conf');

my $log = Log::Log4perl::get_logger();

my $pid          = $PID;
my $test_counter = 0;
my $data_path    = './t/data/publisher';

my $tmp_data_path;
my $irods_tmp_coll;
my $cwc;

# RabbitMQ variables
my @header_keys = qw[timestamp
                     user
                     irods_user
                     type
                     method];
my $expected_headers = scalar @header_keys;
my $test_host = $ENV{'NPG_RMQ_HOST'} || 'localhost';
my $conf = $ENV{'NPG_RMQ_CONFIG'} || './etc/rmq_test_config.json';
my $queue = 'test_irods_data_create_messages';
my $channel = 1;   # TODO increment channel for each test?

sub setup_test : Test(setup) {
  my ($self,) = @_;
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0,
                                );
  $cwc = $irods->working_collection;

  # Prepare a copy of the test data because the tests will modify it
  $tmp_data_path = File::Temp->newdir;
  dircopy($data_path, $tmp_data_path) or
    croak "Failed to copy test data from $data_path to $tmp_data_path";

  $irods_tmp_coll = $irods->add_collection("PublisherTest.$pid.$test_counter");

  # Clear the message queue.
  my $args = $self->rmq_subscriber_args($channel, $conf, $test_host);
  my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
  my @messages = $subscriber->read_all($queue);

  $test_counter++;
}

sub teardown_test : Test(teardown) {
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0,
                                );
  # Delete the copy of the test data
  undef $tmp_data_path;

  $irods->working_collection($cwc);
  $irods->remove_collection($irods_tmp_coll);
}


sub require : Test(1) {
  require_ok('WTSI::NPG::iRODS::PublisherWithReporting');
}

sub message : Test(13) {
  # test RabbitMQ message capability
  my ($self,) = @_;
  my $irods = WTSI::NPG::iRODS->new(environment          => \%ENV,
                                    strict_baton_version => 0,
                                );
  my $publisher = WTSI::NPG::iRODS::PublisherWithReporting->new(
      irods                => $irods,
      routing_key_prefix   => 'test',
      hostname             => $test_host,
      rmq_config_path      => $conf,
      channel              => $channel,
      enable_rmq           => 1,
  );
  my $filename = 'a.txt';
  my $local_file_path  = "$tmp_data_path/publish/$filename";
  my $remote_file_path = "$irods_tmp_coll/$filename";
  my $file_pub = $publisher->publish($local_file_path, $remote_file_path);
  isa_ok($file_pub, 'WTSI::NPG::iRODS::DataObject',
         'publish, file -> returns a DataObject');

  my $args = $self->rmq_subscriber_args($channel, $conf, $test_host);
  my $subscriber = WTSI::NPG::RabbitMQ::TestCommunicator->new($args);
  my @messages = $subscriber->read_all($queue);
  is(scalar @messages, 1, 'Got 1 message from queue');
  my $message = shift @messages;
  my @avus = $irods->get_object_meta($remote_file_path);
  my @acl = $irods->get_object_permissions($remote_file_path);
  my $body =  {avus        => \@avus,
               acl         => \@acl,
               collection  => $irods_tmp_coll,
               data_object => $filename,
           };
  $self->rmq_test_object_message($message, 'publish', $body, $irods);
}
