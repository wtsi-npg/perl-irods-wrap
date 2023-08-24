package WTSI::NPG::iRODS::icommandsTest;

use strict;
use warnings;
use Log::Log4perl;

use base qw(WTSI::NPG::iRODS::Test);
use Test::More;
use Test::Exception;

Log::Log4perl::init('./etc/log4perl_tests.conf');

use WTSI::NPG::iRODS::icommands q[iquest];

sub failed_execution_test : Test(1) {
  my ($self) = @_;

  dies_ok {
    iquest(q[select some invalid command]);
  }, 'Dies on invalid command';
}

sub list_specific_query_test : Test(1) {
  my ($self) = @_;

  ok(iquest('--sql', 'ls'), 'Can list specific queries');
}

sub single_placeholder_test : Test(1) {
  my ($self) = @_;

  my @colls = iquest(qw[%s], q[select COLL_NAME where COLL_NAME like '/testZone/%']);
  is_deeply(\@colls,
     ['/testZone/home',
      '/testZone/home/irods',
      '/testZone/home/public',
      '/testZone/trash',
      '/testZone/trash/home',
      '/testZone/trash/home/irods',
      '/testZone/trash/home/public']) or diag explain \@colls;
}

sub empty_result_test : Test(1) {
  my ($self) = @_;

  my @result = iquest(q[select COLL_NAME where COLL_NAME = 'does not exist']);
  is_deeply(\@result, []) or diag explain \@result;
}

1;
