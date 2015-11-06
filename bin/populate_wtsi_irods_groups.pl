#!/usr/bin/env perl
use strict;
use warnings;
use FindBin qw($Bin);
use lib ( -d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib" );

use autodie;
use Getopt::Long;
use List::MoreUtils qw(uniq);
use Log::Log4perl;
use Log::Log4perl::Level;
use Readonly;

use npg_warehouse::Schema;
use WTSI::NPG::iRODS::GroupAdmin;

our $VERSION = '';

my $embedded_conf = << 'LOGCONF';
   log4perl.logger.npg.irods      = ERROR, A1

   log4perl.appender.A1           = Log::Log4perl::Appender::Screen
   log4perl.appender.A1.utf8      = 1
   log4perl.appender.A1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.A1.layout.ConversionPattern = %d %p %m %n
LOGCONF

my $what_on_earth =<<'WOE';

Script to update WTSI iRODS systems with groups corresponding to
Sequencescape studies.

Appropriate iRODS environment variables (e.g. irodsEnvFile) and files
should be set and configured to allow access and update of the desired
iRODS system.

The Sequencescape warehouse database is used to find the set of
studies. iRODS groups are created for each study with names of the
format ss_<study_id> when they do not already exist.

The iRODS zone is taken to have a pre-existing "public" group which is
used to identify all available users.

If a Sequencescape study has an entry for the "data_access_group" then
the intersection of the members of the corresponding WTSI unix group
and iRODS public group is used as the membership of the corresponding
iRODS group.

If no data_access_group is set on the study, then if the study is
associated with sequencing the members of the iRODS group will be set
to the public group, else if the study is not associated with
sequencing the iRODS group will be left empty (except for the iRODS
groupadmin user).

Script runs to perform such updates when no arguments are given.

Options:

  --debug       Enable debug level logging. Optional, defaults to false.
  --help        Display help.
  --logconf     A log4perl configuration file. Optional.
  --verbose     Print messages while processing. Optional.

WOE

my $debug;
my $log4perl_config;
my $verbose;

GetOptions('debug'     => \$debug,
           'help'      => sub {
             print {*STDERR} $what_on_earth;
             exit 0;
           },
           'logconf=s' => \$log4perl_config,
           'verbose'   => \$verbose);

if ($log4perl_config) {
  Log::Log4perl::init($log4perl_config);
}
else {
  Log::Log4perl::init(\$embedded_conf);
}

my $log = Log::Log4perl->get_logger('npg.irods');
if ($verbose) {
  $log->level($INFO);
}
elsif ($debug) {
  $log->level($DEBUG);
}

my $iga = WTSI::NPG::iRODS::GroupAdmin->new(logger => $log);
my@public=$iga->lg(q(public));
sub _uid_to_irods_uid {
  my($u)=@_;
  return grep {/^\Q$u\E#/smx} @public;
}

Readonly::Scalar my $GROUP_SECONDARY_MEMBERS_FIELD_INDEX => 3;
my%ug2id; #cache of group to users - populate here
my%gid2group;
open my$gfh, q(-|), q(getent group);
while(<$gfh>){
  chomp;
  $log->debug("getent group: " . $_);
  my@F=split /:/smx;
  my$users=$ug2id{$F[0]}||=[];
  push @{$users}, split /,/smx, $F[$GROUP_SECONDARY_MEMBERS_FIELD_INDEX]||q(); #fill with secondary groups for users
  $gid2group{$F[2]}=$F[0];
}
close $gfh;
Readonly::Scalar my $PASSWD_PRIMARY_GID_FIELD_INDEX => 3;
open my$pfh, q(-|), q(getent passwd);
while(<$pfh>){
  chomp;
  $log->debug("getent passwd: " . $_);
  my@F=split /:/smx;
  push @{$ug2id{$gid2group{$F[$PASSWD_PRIMARY_GID_FIELD_INDEX]}||=q()}},$F[0]; #fill with primary group for users - empty strong used if no group found for gid
}
close $pfh;
foreach my$users (values%ug2id){
  $users=[uniq@{$users}];
}

my $s=npg_warehouse::Schema->connect();
my$rs=$s->resultset(q(CurrentStudy));

my($group_count,$altered_count)= (0,0);
while (my$st=$rs->next){
  my$study_id=$st->internal_id;
  my$gs=$st->data_access_group();
  $log->debug("Working on study $study_id, SScape data access: ",
              defined $gs ? $gs : q());
  my@g= defined $gs ? $gs=~m/\S+/smxg : ();
  my$is_seq=($st->npg_information->count||$st->npg_plex_information->count)>0;
  my@m=@g      ? map{ _uid_to_irods_uid($_) } map { @{ $ug2id{$_} || [$_] } } @g : #if strings from data access group don't match any group name try treating as usernames
       $is_seq ? @public :
       ();
  $log->debug('Membership: ', join q(, ), @m);
  $altered_count += $iga->set_group_membership("ss_$study_id",@m) ? 1 : 0;
  $group_count++;
}

$log->debug("Altered $altered_count groups");

if($altered_count){
  $log->info("When considering $group_count Sequencescape studies, ",
             "$altered_count iRODS groups were created or their ",
             'membership altered (by ', $iga->_user, ')');
}
