#!/usr/bin/env perl
use strict;
use warnings;
use English qw(-no_match_vars);
use FindBin qw($Bin);
use lib ( -d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib" );

use autodie;
use Getopt::Long;
use List::MoreUtils qw(uniq);
use Log::Log4perl qw(:levels);
use Net::LDAP;
use Readonly;

use WTSI::DNAP::Warehouse::Schema;
use WTSI::NPG::iRODS::GroupAdmin;

our $VERSION = '';

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
  --dry_run
  --help        Display help.
  --logconf     A log4perl configuration file. Optional.
  --study       Restrict updates to a study identifier. May be used multiple
                times to select more than one study. Optional.
  --verbose     Print messages while processing. Optional.

WOE

my $debug;
my $dry_run;
my $log4perl_config;
my $verbose;
my @study_ids;

GetOptions('debug'                   => \$debug,
           'dry-run|dry_run'         => \$dry_run,
           'help'                    => sub {
             print $what_on_earth;
             exit 0;
           },
           'logconf=s'               => \$log4perl_config,
           'study=i'                 => \@study_ids,
           'verbose'                 => \$verbose) or die "\n$what_on_earth\n";

if ($log4perl_config) {
  Log::Log4perl::init($log4perl_config);
}
else {
  my $log_args = {layout => '%d %p %m %n',
                  level  => $ERROR,
                  utf8   => 1};

  if ($verbose || ($dry_run && !$debug)) {
    $log_args->{level} = $INFO;
  }
  elsif ($debug) {
    $log_args->{level} = $DEBUG;
  }

  Log::Log4perl->easy_init($log_args);
}

my $log = Log::Log4perl->get_logger('main');

my $iga = WTSI::NPG::iRODS::GroupAdmin->new(dry_run => $dry_run);

my @public = $iga->lg(q(public));
$log->info("The iRODS public group has ", scalar @public, ' members');
$log->debug("iRODS public group membership: ", join q(, ), @public);

sub _uid_to_irods_uid {
  my($u)=@_;
  return grep {/^\Q$u\E#/smx} @public;
}

my $host = 'ldap.internal.sanger.ac.uk';
my $ldap = Net::LDAP->new($host);

$ldap->bind or $log->logcroak("LDAP failed to bind to '$host': ", $!);
# Get group, gid and member uids from LDAP
my ($group2uids, $gid2group) = find_group_ids($ldap);
# Get uids and their primary gid from LDAP
my $uid2gid = find_primary_gid($ldap);
$ldap->unbind or $log->logwarn("LDAP failed to unbind '$host': ", $!);

# For each uid, merge primary gid with secondary gids
foreach my $uid (keys %{$uid2gid}) {
  my $gid           = $uid2gid->{$uid};
  my $primary_group = $gid2group->{$gid};

  # Some users in LDAP have a gidNumber that does not correspond to a
  # Unix group
  if (defined $primary_group) {
    push @{$group2uids->{$primary_group}}, $uid;
  }
}

foreach my $group (keys %{$group2uids}){
  my @uids = uniq @{$group2uids->{$group}};
  $group2uids->{$group} = \@uids;
  $log->debug("Group '$group' membership ", join q(, ), @uids);
}

my $mlwh = WTSI::DNAP::Warehouse::Schema->connect;
my $query = @study_ids ? {id_study_lims => \@study_ids} : {};
my $studies = $mlwh->resultset('Study')->search($query,
                                                {order_by => 'id_study_lims'});

my ($group_count, $altered_count) = (0, 0);
while (my $study = $studies->next){
  my $study_id = $study->id_study_lims;
  my $dag_str  = $study->data_access_group || q();
  my $is_seq   = $study->iseq_flowcells->count ||
                 $study->pac_bio_runs->count;

  $log->debug("Working on study $study_id, SScape data access: '$dag_str'");

  my @members;
  my @dags = $dag_str =~ m/\S+/smxg;
  if (@dags) {
    # if strings from data access group don't match any group name try
    # treating as usernames
    @members = map { _uid_to_irods_uid($_) }
               map { @{ $group2uids->{$_} || [$_] } } @dags;
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

# Find both gid and member uids for each group
sub find_group_ids {
  my ($ld) = @_;

  my $query_base   = 'ou=group,dc=sanger,dc=ac,dc=uk';
  my $query_filter = '(cn=*)';
  my $search = $ld->search(base   => $query_base,
                           filter => $query_filter);
  if ($search->code) {
    $log->logcroak("LDAP query base: '$query_base', filter: '$query_filter' ",
                   'failed: ', $search->error);
  }

  my %group2uids;
  my %gid2group;
  foreach my $entry ($search->entries) {
    my $group   = $entry->get_value('cn');
    my $gid     = $entry->get_value('gidNumber');
    my @uids    = $entry->get_value('memberUid');

    $group2uids{$group} = \@uids;
    $gid2group{$gid}    = $group;
  }

  return (\%group2uids, \%gid2group);
}

sub find_primary_gid {
  my ($ld) = @_;

  my $query_base   = 'ou=people,dc=sanger,dc=ac,dc=uk';
  my $query_filter = '(sangerActiveAccount=TRUE)';
    my $search = $ld->search(base   => $query_base,
                             filter => $query_filter);
  if ($search->code) {
    $log->logcroak("LDAP query base: '$query_base', filter: '$query_filter' ",
                   'failed: ', $search->error);
  }

  my %uid2gid;
  foreach my $entry ($search->entries) {
    $uid2gid{$entry->get_value('uid')} = $entry->get_value('gidNumber');
  }

  return \%uid2gid;
}
