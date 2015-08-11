#!/usr/bin/env perl
use strict;
use warnings;
use FindBin qw($Bin);
use lib ( -d "$Bin/../lib/perl5" ? "$Bin/../lib/perl5" : "$Bin/../lib" );
use WTSI::NPG::iRODS::GroupAdmin;
use npg_warehouse::Schema;
use autodie;
use List::MoreUtils qw(uniq);

our $VERSION = '';

my $what_on_earth =<<'WOE';

Script to update WTSI iRODS systems with groups corresponding to Sequencescape studies.

Appropriate iRODS environment variables (e.g. irodsEnvFile) and files should be set and configured to allow access and update of the desired iRODS system.

The Sequencescape warehouse database is used to find the set of studies. iRODS groups are created for each study with names of the format ss_<study_id> when they do not already exist.

The iRODS zone is taken to have a pre-existing "public" group which is used to identify all available users.

If a Sequencescape study has an entry for the "data_access_group" then the intersection of the members of the corresponding WTSI unix group and iRODS public group is used as the membership of the corresponding iRODS group.

If no data_access_group is set on the study, then if the study is associated with sequencing the members of the iRODS group will be set to the public group, else if the study is not associated with sequencing the iRODS group will be left empty (except for the iRODS groupadmin user).

Script runs to perform such updates when no arguments are given.

WOE


if(@ARGV){
  print {*STDERR} $what_on_earth;
  exit 0;
}

my $iga = WTSI::NPG::iRODS::GroupAdmin->new();
my@public=$iga->lg(q(public));
sub _uid_to_irods_uid {
  my($u)=@_;
  return grep {/^\Q$u\E#/smx} @public;
}

my%ug2id; #cache of group to users - populate here
my%gid2group;
open my$gfh, q(-|), q(getent group);
while(<$gfh>){
  chomp;
  my@F=split q(:);
  my$users=$ug2id{$F[0]}||=[];
  push @{$users}, split q(,),$F[3]||q(); #fill with secondary groups for users
  $gid2group{$F[2]}=$F[0];
}
close $gfh;
open my$pfh, q(-|), q(getent passwd);
while(<$pfh>){
  chomp;
  my@F=split q(:);
  push @{$ug2id{$gid2group{$F[3]}||=q()}},$F[0]; #fill with primary group for users - empty strong used if no group found for gid
}
close $pfh;
foreach my$users (values%ug2id){
  $users=[uniq@{$users}];
}

sub ug2id {
  my$g=shift||return;
  if(my$gha=$ug2id{$g}){return @{$gha};}
  return;
}

my $s=npg_warehouse::Schema->connect();
my$rs=$s->resultset(q(CurrentStudy));

my($group_count,$altered_count)= (0,0);
while (my$st=$rs->next){
  my$study_id=$st->internal_id;
  my$gs=$st->data_access_group();
  my@g= defined $gs ? $gs=~m/\S+/smxg : ();
  my$is_seq=($st->npg_information->count||$st->npg_plex_information->count)>0;
  my@m=@g      ? map{ _uid_to_irods_uid($_) } map { ug2id($_) } @g :
       $is_seq ? @public :
                 ();
  $altered_count += $iga->set_group_membership("ss_$study_id",@m) ? 1 : 0;
  $group_count++;
}

if($altered_count){
  print {*STDERR} "When considering $group_count Sequencescape studies, $altered_count iRODS groups were created or their membership altered (by ".($iga->_user).")\n";
}
