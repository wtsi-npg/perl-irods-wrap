package WTSI::NPG::iRODS::GroupAdmin;

use namespace::autoclean;
use Data::Dump qw(pp);
use Moose;
use MooseX::StrictConstructor;
use IPC::Run qw(start run);
use File::Which qw(which);
use Cwd qw(abs_path);
use List::MoreUtils qw(any none);
use Log::Log4perl;
use Readonly;
use Carp;

use WTSI::NPG::iRODS;

our $VERSION = '';

=head1 NAME

WTSI::NPG::iRODS::GroupAdmin

=head1 SYNOPSIS

  use WTSI::NPG::iRODS::GroupAdmin;
  my $iga = WTSI::NPG::iRODS::GroupAdmin->new();
  print join",",$iga->lg;
  print join",",$iga->lg(q(public));

=head1 DESCRIPTION

A class for running iRODS group admin related commands for creating groups and altering their membership

=head1 SUBROUTINES/METHODS

=cut

Readonly::Scalar our $IGROUPADMIN => q(igroupadmin);
Readonly::Scalar our $IENV => q(ienv);

with 'WTSI::DNAP::Utilities::Loggable';

has 'dry_run' =>
  (is      => 'ro',
   isa     => 'Bool',
   default => 0);

has '_in' => (
  'is' => 'ro',
  'isa' => 'ScalarRef[Str]',
  'default' => sub {my$t=q{}; return \$t},
);

has '_out' => (
  'is' => 'ro',
  'isa' => 'ScalarRef[Str]',
  'default' => sub {my$t=q{}; return \$t},
);

has '_running' => (
    'is'       => 'rw',
    'isa'      => 'Bool',
    'required' => 1,
    'default' => 0,
);

has '_harness' => (
  'is' => 'ro',
  'builder' => '_build__harness',
  'lazy' => 1, #lazy as we need _in and _out to be instantiated before creating the harness
);

sub _build__harness {
  my ($self) = @_;
  my $in_ref = $self->_in;
  ${$in_ref} = "\n"; #prevent initial hang - fetch the chicken...
  my $out_ref = $self->_out;
  # workaround Run::IPC caching : https://rt.cpan.org/Public/Bug/Display.html?id=57393
  my $cmd = which $IGROUPADMIN;
  if (not $cmd) {
    $self->logcroak(qq(Command '$IGROUPADMIN' not found));
  }
  my $h = start [abs_path $cmd], q(<pty<), $in_ref, q(>pty>), $out_ref;
  $self->_running(1);
  $self->_pump_until_prompt($h);
  ${$out_ref}=q();
  return $h;
}

sub _pump_until_prompt {
  my($self,$h)=@_;
  $h ||= $self->_harness;
  while ($self->_running){
    $h->pump;
    last if ${$self->_out}=~s/\r?\n^groupadmin\>//smx;
  }
  return;
}

sub _push_pump_trim_split {
  my($self,$in)=@_;
  my $out_ref = $self->_out;
  ${$out_ref}=q();
  ${$self->_in} = $in;
  $self->_pump_until_prompt();
  ${$out_ref}=~s/\r//smxg; #igroupadmin inserts CR before LF - remove all
  ${$out_ref}=~s/\A\Q$in\E//smx;
  if ( ${$out_ref}=~m/^ERROR:[^\n]+\z/smx ) {
    $self->logcroak('igroupadmin error: ', ${$out_ref});
  }
  my@results=split /\n/smx, ${$out_ref};
  ${$out_ref}=q();
  return @results;
}

sub __croak_on_bad_group_name {
  my($self, $group)=@_;
  if( (not defined $group ) or $group eq q()){
      $self->logcroak(q(empty string group name does not make sense to iRODs));
  }elsif ($group =~ /"/smx){
      $self->logcroak(qq(Cannot cope with group names containing double quotes '"' : $group));
  }
  return;
}

=head2 lg

List groups if not argument given, or list members of the group given as argument.

=cut

sub lg {
  my($self,$group)=@_;
  my $in = q(lg);
  if(defined $group){
    $self->__croak_on_bad_group_name($group);
    $in .= qq( "$group");
  }
  $in .= qq(\n);
  my @results = $self->_push_pump_trim_split($in);
  if(defined $group){
    my $leadingtext = shift @results;
    if( @results and not $leadingtext=~/\AMembers\sof\sgroup/smx) {
      $self->logcroak(qq(unexpected text: \"$leadingtext\"));
    }
  }
  if (@results==1 and $results[0]=~/\ANo\srows\sfound/smx ){
    shift @results;
    if (@results==0 and defined $group and none {$group eq $_} $self->lg){
      $self->logcroak(qq(group "$group" does not exist));
    }
  }
  return @results;
}


has '_user' => (
  'is' => 'ro',
  'isa' => 'Str',
  'builder' => '_build__user',
);
sub _build__user {
  my ($self) = @_;

  my $irods = WTSI::NPG::iRODS->new;
  my $env = $irods->get_irods_env;
  my $user = $env->{irods_user_name};
  my $zone = $env->{irods_zone_name};

  if ($user and $zone) {
    return "$user#$zone";
  } else {
    $self->logcroak('Could not obtain user and zone from ienv ', pp($env));
  }

  return;
}

sub _op_g_u {
  my($self,$op,$group,$user)=@_;
  $self->__croak_on_bad_group_name($group);
  if( (not defined $user ) or $user eq q()){
      $self->logcroak(q(empty string username does not make sense to iRODs));
  }elsif ($user =~ /"/smx){
    $self->logcroak(qq(Cannot cope with username containing double quotes '"' : $group));
  }
  my $in = qq($op "$group" "$user"\n);

  $self->_push_pump_trim_split($in);

  return;
}

sub _ensure_existence_of_group {
  my($self,$group)=@_;
  $self->__croak_on_bad_group_name($group);
  if ( any {$group eq $_} $self->lg){ return;}
  if ($self->dry_run) {
    $self->info("Dry run: mkgroup '$group'");
  }
  else {
    $self->_push_pump_trim_split(qq(mkgroup "$group"\n));
  }
  return 1; #return true if we make a group
}

=head2 set_group_membership

Given a group and list of members will ensure that the group exists and contains exactly this admin user and the members (adding or removing as appropriate). Return true if a group is created or its membership altered.

=cut

sub set_group_membership {
  my($self,$group,@members)=@_;
  my $altered = $self->_ensure_existence_of_group($group);
  my @orig_members = $self->lg($group);
  $self->debug("Members of $group: ", join q(, ), @orig_members);
  if (@orig_members){
    if(none {$_ eq $self->_user} @orig_members) {carp "group $group does not contain user ".($self->_user).': authorization failure likely';}
  }else{
    if (not $self->dry_run) {
      $self->_op_g_u('atg',$group, $self->_user); #add this user to empty group (first) so admin rights to operate on it are retained
    }
    push @orig_members, $self->_user;
    $altered = 1;
  }
  my%members = map{$_=>1}@members,$self->_user;
  @orig_members = grep{ not delete $members{$_}} @orig_members; #make list to delete from orginal members if not in new list, leaves member to add in hash
  foreach my $m (@orig_members){
    if ($self->dry_run) {
      $self->info("Dry run: removing $m from $group");
    }
    else {
      $self->info("Removing $m from $group");
      $self->_op_g_u('rfg',$group,$m);
    }
  }
  $altered ||= @orig_members;
  @members = keys %members;
  foreach my $m (@members) {
    if ($self->dry_run) {
      $self->info("Dry run: adding $m to $group");
    }
    else {
      $self->info("Adding $m to $group");
      $self->_op_g_u('atg',$group,$m);
    }
  }
  $altered ||= @members;
  if ($self->dry_run) {
    $self->info("Dry run: altered $altered members of $group");
  }
  else {
    $self->info("Altered $altered members of $group");
  }
  return $altered;
}

sub BUILD {
  my ($self) = @_;
  $self->_harness; #ensure we start igroupadmin at object creation (and so with expected environment: environment variables used by igroupadmin)
  return;
}

sub DEMOLISH {
  my ($self) = @_;
  $self->_running(0);
  if($self->_out and $self->_in){
    ${$self->_out}=q();
    ${$self->_in}="quit\n";
    $self->_harness->finish;
  }
  return;
}

no Moose;

1;
__END__

=head1 DIAGNOSTICS

=head1 CONFIGURATION AND ENVIRONMENT

Will honour iRODS related environment at time of object creation

=head1 DEPENDENCIES

=over

=item strict

=item warnings

=item Moose

=item IPC::Run

=item File::Which

=item Cwd

=item List::MoreUtils

=item Readonly

=item Carp

=back

=head1 INCOMPATIBILITIES

=head1 BUGS AND LIMITATIONS

  In ad-hoc testing different numbers of results for the same query have been seen - but v rarely and not reproducibly!

=head1 AUTHOR

David K. Jackson <david.jackson@sanger.ac.uk>
Keith James <kdj@sanger.ac.uk>

=head2 LICENSE AND COPYRIGHT

Copyright (C) 2013, 2014, 2016 Genome Research Limited. All Rights
Reserved.

This program is free software: you can redistribute it and/or modify
it under the terms of the Perl Artistic License or the GNU General
Public License as published by the Free Software Foundation, either
version 3 of the License, or (at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

=cut
