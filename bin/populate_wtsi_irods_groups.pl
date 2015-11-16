#!/usr/bin/env perl
use strict;
use warnings;
use English qw(-no_match_vars);
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
  --dry-run     Report proposed changes, do not perform them. Optional.
  --help        Display help.
  --logconf     A log4perl configuration file. Optional.
  --study       Restrict updates to a study. May be used multiple times
                to select more than one study. Optional.
  --verbose     Print messages while processing. Optional.

WOE

Readonly::Scalar my $GETENT_GROUP_ALERT_THRESH  => 200;
Readonly::Scalar my $GETENT_PASSWD_ALERT_THRESH => 5000;

my $debug;
my $dry_run;
my $log4perl_config;
my $verbose;
my @studies;

GetOptions('debug'             => \$debug,
           'dry-run|dry_run'   => \$dry_run,
           'help'              => sub {
             print $what_on_earth;
             exit 0;
           },
           'logconf=s'         => \$log4perl_config,
           'study=s'           => \@studies,
           'verbose'           => \$verbose) or die "\n$what_on_earth\n";

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
if ($debug) {
  $log->level($DEBUG);
}

my $iga = WTSI::NPG::iRODS::GroupAdmin->new(dry_run => $dry_run,
                                            logger  => $log);

my @public = $iga->lg(q(public));
$log->info("The iRODS public group has ", scalar @public, ' members');
$log->debug("iRODS public group membership: ", join q(, ), @public);

sub _uid_to_irods_uid {
  my($u)=@_;
  return grep {/^\Q$u\E#/smx} @public;
}

Readonly::Scalar my $GROUP_SECONDARY_MEMBERS_FIELD_INDEX => 3;
my%ug2id; #cache of group to users - populate here
my%gid2group;
my $num_group_lines = 0;
open my$gfh, q(-|), q(getent group) or
  $log->logcroak("Opening pipe to getent group failed: $ERRNO");
while(<$gfh>){
  $num_group_lines++;
  chomp;
  $log->debug("getent group: ", $_);
  my@F=split /:/smx;
  my$users=$ug2id{$F[0]}||=[];
  push @{$users}, split /,/smx, $F[$GROUP_SECONDARY_MEMBERS_FIELD_INDEX]||q(); #fill with secondary groups for users
  $gid2group{$F[2]}=$F[0];
}
close $gfh or
  $log->logcroak("Closing pipe to getent group failed: $ERRNO");

if ($num_group_lines < $GETENT_GROUP_ALERT_THRESH) {
  $log->logcroak("Output of 'getent group' appears truncated ",
                 "($num_group_lines lines)");
}

Readonly::Scalar my $PASSWD_PRIMARY_GID_FIELD_INDEX => 3;

my $num_passwd_lines = 0;
open my$pfh, q(-|), q(getent passwd) or
  $log->logcroak("Opening pipe to getent passwd failed: $ERRNO");
while(<$pfh>){
  $num_passwd_lines++;
  chomp;
  $log->debug("getent passwd: ", $_);
  my@F=split /:/smx;
  push @{$ug2id{$gid2group{$F[$PASSWD_PRIMARY_GID_FIELD_INDEX]}||=q()}},$F[0]; #fill with primary group for users - empty strong used if no group found for gid
}
close $pfh or
  $log->logcroak("Closing pipe to getent passwd failed: $ERRNO");

if ($num_passwd_lines < $GETENT_PASSWD_ALERT_THRESH) {
  $log->logcroak("Output of 'getent passwd' appears truncated ",
                 "($num_passwd_lines lines)");
}

foreach my $users (values%ug2id){
  $users = [uniq @{$users}];
}

my $schema = npg_warehouse::Schema->connect;
my $rs;
if (@studies) {
  $rs = $schema->resultset(q(CurrentStudy))->search({internal_id => \@studies});
}
else {
  $rs = $schema->resultset(q(CurrentStudy));
}

my ($group_count, $altered_count) = (0, 0);
while (my $study = $rs->next){
  my $study_id = $study->internal_id;
  my $dag_str  = $study->data_access_group || q();
  my $is_seq   = $study->npg_information->count ||
                 $study->npg_plex_information->count;

  $log->debug("Working on study $study_id, SScape data access: '$dag_str'");

  my @members;
  my @dags = $dag_str =~ m/\S+/smxg;
  if (@dags) {
    # if strings from data access group don't match any group name try
    # treating as usernames
    @members = map { _uid_to_irods_uid($_)   }
               map { @{ $ug2id{$_} || [$_] } } @dags;
  }
  elsif ($is_seq) {
    @members = @public;
  }
  else {
    # remains empty
  }

  $log->info("Study $study_id has ", scalar @members, ' members');
  $log->debug('Members: ', join q(, ), @members);

  if ($iga->set_group_membership("ss_$study_id", @members)) {
    $altered_count++;
  }

  $group_count++;
}

$log->debug("Altered $altered_count groups");

$log->info("When considering $group_count Sequencescape studies, ",
           "$altered_count iRODS groups were created or their ",
           'membership altered (by ', $iga->_user, ')');
