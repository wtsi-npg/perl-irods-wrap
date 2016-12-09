package WTSI::NPG::iRODS::AnnotatorTest;

use strict;
use warnings;
use Log::Log4perl;
use Test::More;
use Test::Exception;
use WTSI::NPG::iRODS::Metadata;
use URI;

use base qw[WTSI::NPG::iRODS::Test];

use WTSI::NPG::iRODS::Metadata;
use WTSI::NPG::iRODS::Annotator;

Log::Log4perl::init('./etc/log4perl_tests.conf');

{
  package TestAnnotator;
  use Moose;

  with 'WTSI::NPG::iRODS::Annotator';
}

sub make_creation_metadata : Test(4) {
  my $uri = URI->new("http:");
  $uri->host('www.sanger.ac.uk');

  my $creator    = $uri;
  my $time       = DateTime->now;
  my $publisher  = $uri;

  my @expected = ({attribute => $DCTERMS_CREATOR,
                   value     => $creator->as_string},
                  {attribute => $DCTERMS_CREATED,
                   value     => $time->iso8601},
                  {attribute => $DCTERMS_PUBLISHER,
                   value     => $publisher->as_string});
  my @observed = TestAnnotator->new->make_creation_metadata
    ($creator, $time, $publisher);

  is_deeply(\@observed, \@expected) or diag explain \@observed;

  dies_ok {
    TestAnnotator->new->make_creation_metadata(undef, $time, $publisher);
  } 'Dies on undefined creator';

  dies_ok {
    TestAnnotator->new->make_creation_metadata($creator, undef, $publisher);
  } 'Dies on undefined creation time';

  dies_ok {
    TestAnnotator->new->make_creation_metadata($creator, $time, undef);
  } 'Dies on undefined publisher';
}

sub make_modification_metadata : Test(2) {
  my $time = DateTime->now;

  my @expected = ({attribute => $DCTERMS_MODIFIED,
                   value     => $time->iso8601});
  my @observed = TestAnnotator->new->make_modification_metadata($time);

  is_deeply(\@observed, \@expected) or diag explain \@observed;

  dies_ok { TestAnnotator->new->make_modification_metadata }
    'Dies on undefined modification time';
}

sub make_type_metadata : Test(12) {

  # Some of these don't make pactical sense, but should be allowed if
  # encountered
  my %expected =
    (
     'test.txt'            => 'txt',
     'test.txt.gz'         => 'txt.gz',
     'test.txt.gz.bz2'     => 'txt.gz.bz2',
     'test.txt.gz.bz2.tar' => 'tar',
     'test.foo.txt'        => 'txt',
     'test.gz.txt'         => 'txt',
     'gz.txt'              => 'txt',
     'test.custom1'        => 'custom1',
     'test.custom2'        => 'custom2',
     'test.custom1.gz'     => 'custom1.gz',
     'test.custom2.gz'     => 'custom2.gz',
    );

  while (my ($file, $suffix) = each %expected) {
    my @expected_type = ({attribute => $FILE_TYPE,
                          value     => $suffix});
    my @observed_type = TestAnnotator->new->make_type_metadata($file,
                                                               'custom1',
                                                               'custom2');

    is_deeply(\@observed_type, \@expected_type,
              "File '$file' has type suffix '$suffix'")
      or diag explain \@observed_type;
  }

  my @no_type = TestAnnotator->new->make_type_metadata('test.z', '.a', '.b');
  is_deeply(\@no_type, [], 'No type metadata if type not recognised')
    or diag explain \@no_type;
}

sub make_md5_metadata : Test(2) {
  my $md5 = '68b329da9893e34099c7d8ad5cb9c940';
  my @expected = ({attribute => $FILE_MD5,
                   value     => $md5});
  my @observed = TestAnnotator->new->make_md5_metadata($md5);

  is_deeply(\@observed, \@expected) or diag explain \@observed;

  dies_ok {
    TestAnnotator->new->make_md5_metadata;
  } 'Dies on undefined MD5';
}

sub make_ticket_metadata : Test(2) {
  my $ticket = '0123456789';
  my @expected = ({attribute => $RT_TICKET,
                   value     => $ticket});
  my @observed = TestAnnotator->new->make_ticket_metadata($ticket);

  is_deeply(\@observed, \@expected) or diag explain \@observed;

  dies_ok {
    TestAnnotator->new->make_ticket_metadata;
  } 'Dies on undefined ticket';
}

1;
