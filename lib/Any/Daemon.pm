# This code is part of distribution Any-Daemon. Meta-POD processed with
# OODoc into POD and HTML manual-pages.  See README.md
# Copyright Mark Overmeer.  Licensed under the same terms as Perl itself.

package Any::Daemon;

use warnings;
use strict;

use Log::Report::Optional  'any-daemon';

use POSIX         qw(setsid setuid setgid :sys_wait_h);
use English       qw/$EUID $EGID $PID/;
use File::Spec    ();

use constant
  { SLEEP_FOR_SOME_TIME   =>  10
  , ERROR_RECOVERY_SLEEP  =>   5
  , SLOW_WARN_AGAIN_AFTER => 300
  };

# One program can only run one daemon
my %childs;

=chapter NAME
Any::Daemon - basic needs for a daemon

=chapter SYNOPSIS
  use Any::Daemon;
  use Log::Report;

  # Prepare a daemon for the Operating System
  my $daemon = Any::Daemon->new(@os_opts);

  # Start logging to syslog (see Log::Report::Dispatcher)
  dispatcher SYSLOG => 'syslog';

  # Run managing daemon
  $daemon->run(@run_opts);

=chapter DESCRIPTION
This module delivers the basic needs for any daemon on UNIX systems.
There are other standard daemon implementations available on CPAN,
with as main common difference that this module is not dedicated to a
specific task. By using M<Log::Report>, you can easily redirect error
reports to any logging mechanism you like.

The code for this module is in use for many different daemons, some
with heavy load (a few dozen requests per second)  Have a look in
the examples directory!  Also, you may like M<Any::Daemon::HTTP>

=chapter METHODS

=section Constructors

=c_method new %options
With C<new()> you provide the operating system integration %options,
where C<run()> gets the activity related parameters: the real action.

Be warned that the user, group, and workdir will not immediately be
effected: delayed until M<run()>.

=option  pid_file FILENAME
=default pid_file C<undef>

=option  user  UID|USERNAME
=default user  C<undef>
Change to this user (when started as root)  If you want to run your
daemon as root, then explicitly specify that with this option, to
avoid a warning.

=option  group GID|GROUPNAME
=default group C<undef>
Change to this group (when started as root)

=option  workdir DIRECTORY
=default workdir current working directory
Change DIRECTORY so temporary files and such are not written in the
random directory where the daemon got started.

If the directory does not exist yet, it will be created with mode 0700
when the daemon object is initialized. We only move to that directory
when the daemon is run. The working directory does not get cleaned when
the daemon stops.

=cut

sub new(@) {my $class = shift; (bless {}, $class)->init({@_})}

sub init($)
{   my ($self, $args) = @_;
    $self->{AD_pidfn} = $args->{pid_file};

    my $user = $args->{user};
    if(defined $user)
    {   if($user =~ m/[^0-9]/)
        {   my $uid = $self->{AD_uid} = getpwnam $user;
            defined $uid
                or error __x"user {name} does not exist", name => $user;
        }
        else { $self->{AD_uid} = $user }
    }
    elsif($EUID==0)
    {   warning __"running daemon as root is dangerous: please specify user";
    }

    my $group = $args->{group};
    if(defined $group)
    {   if($group =~ m/[^0-9]/)
        {   my $gid = $self->{AD_gid} = getgrnam $group;
            defined $gid
                or error __x"group {name} does not exist", name => $group;
        }
    }

    $self->{AD_wd} = $args->{workdir};
    $self;
}

#--------------------
=section Accessors

=method workdir
[0.90] assigned working directory of the daemon in the file-system.

=method pidFilename
=cut

sub workdir() {shift->{AD_wd}}
sub pidFilename() { shift->{AD_pidfn} }

#--------------------
=section Action

=method run %options
The C<run> method gets the activity related parameters.

You specify either C<run_task>, for the function to be called by this
deamon itself, or C<child_task> when you wish to manage child tasks
which run the action.

[0.96] When you pass a method name, it will be called on this object.
This is very clean when you have extended this daemon class.

=option  background BOOLEAN
=default background <true>
Run the managing daemon in the background. During testing, it is
prefered to run the daemon in the foreground, to be able to stop
the daemon with Crtl-C and to see errors directly on the screen
in stead of only in some syslog file.

=option  run_task CODE|METHOD
=default run_task C<undef>
[0.96] The CODE which will be run by this process.  This implies: no
managed children.

=option  child_task CODE|METHOD
=default child_task C<undef>
The CODE will be run for each child which is started, also when they
are started later on. If the task is not specified, only a warning is
produced. This may be useful when you start implementing the daemon:
you do not need to care about the task to perform yet.

The returned value of thise CODE is used as exit code of the child
process, where zero means 'ok'.

=option  kill_childs CODE|METHOD
=default kill_childs 'killChilds'
The CODE terminates all running children, maybe to start new ones,
maybe to terminate the whole daemon.

=option  child_died CODE|METHOD
=default child_died 'childDied'
The C<child_died> routine handles dieing kids and the restart of new
ones.  It gets two parameters: the maximum number of childs plus the
task to perform per kid.

=option  reconfigure CODE|METHOD
=default reconfigure 'reconfigDaemon'
The CODE is run when a SIGHUP is received; signal 1 is used by most
daemons as trigger for reconfiguration.

=option  max_childs INTEGER
=default max_childs 10
The maximum (is usual) number of childs to run.
=cut

sub _mkcall($)
{   return $_[1] if ref $_[1] eq 'CODE';
    my ($self, $what) = @_;
    sub { $self->$what(@_) };
}

sub run(@)
{   my ($self, %args) = @_;

    my $wd = $self->workdir;
    if($wd)
    {   -d $wd or mkdir $wd, 0700
            or fault __x"cannot create working directory {dir}", dir => $wd;

        chdir $wd
            or fault __x"cannot change to working directory {dir}", dir => $wd;
    }

    my $bg = exists $args{background} ? $args{background} : 1;
    if($bg)
    {   trace "backgrounding managing daemon";

        my $kid = fork;
        if($kid)
        {   # starting parent is ready to leave
            exit 0;
        }
        elsif(!defined $kid)
        {   fault __x"cannot start the managing daemon";
        }

        dispatcher('list') >= 1
            or error __x"you need to have a dispatcher to send log to";
    }

    my $pidfn = $self->pidFilename;
    if(defined $pidfn)
    {   local *PIDF;
        if(open PIDF, '>', $pidfn)
        {   print PIDF "$PID\n";
            close PIDF;
        }
    }

    my $gid = $self->{AD_gid} || $EGID;
    my $uid = $self->{AD_uid} || $EUID;

    chown $uid,$gid, $wd if $wd;   # don't check success: user may have plan

    if($gid != $EGID)
    {   if($] > 5.015007)
        {   setgid $gid or fault __x"cannot change to group {gid}", gid => $gid;
        }
        else   # in old versions of Perl, the uid and gid gets cached
        {   eval { $EGID = $gid };
            $@ and error __x"cannot switch to group {gid}: {err}"
               , gid => $gid, err => $@;
        }
    }

    if($uid != $EUID)
    {   if($] > 5.015007)
        {   setuid $uid or fault __x"cannot change to user {uid}", uid => $uid;
        }
        else
        {   eval { $EUID = $uid };
            $@ and error __x"cannot switch to user {uid}: {err}"
               , uid => $uid, err => $@;
        }
    }

    setsid;

    my $child_task  = $self->_mkcall($args{child_task});
    my $own_task    = $self->_mkcall($args{run_task});

    $child_task || $own_task
        or panic __x"you have to run with either child_task or run_task";

    $child_task && $own_task
        or panic __x"run with only one of child_task and run_task";

    if($bg)
    {   # no standard die and warn output anymore (Log::Report)
        dispatcher close => 'default';

        # to devnull to avoid write errors in third party modules
        open STDIN,  '<', File::Spec->devnull;
        open STDOUT, '>', File::Spec->devnull;
        open STDERR, '>', File::Spec->devnull;
    }

    if($child_task)
         { $self->_run_with_childs($child_task, %args) }
    else { $self->_run_without_childs($own_task, %args) }
}

sub _run_with_childs($%) {
    my ($self, $child_task, %args) = @_;
    my $reconfig    = $self->_mkcall($args{reconfig}    || 'reconfigDaemon');
    my $kill_childs = $self->_mkcall($args{kill_childs} || 'killChilds');
    my $child_died  = $self->_mkcall($args{child_died}  || 'childDied');
    my $max_childs  = $args{max_childs}  || 10;

    my $run_child   = sub
      { # re-seed the random number sequence per process
        srand(time+$$);

        # unhandled errors are to be treated seriously.
        my $rc = try { $child_task->(@_) };
        if(my $e = $@->wasFatal) { $e->throw(reason => 'ALERT'); $rc = 1 }
        $rc;
      };

    $SIG{CHLD} = sub { $child_died->($max_childs, $run_child) };
    $SIG{HUP}  = sub
      { notice "daemon received signal HUP";
        $reconfig->(keys %childs);
        $child_died->($max_childs, $run_child);
      };

    $SIG{TERM} = $SIG{INT} = sub
      { my $signal = shift;
        notice "daemon terminated by signal $signal";

        $SIG{TERM} = $SIG{CHLD} = 'IGNORE';
        $max_childs = 0;
        $kill_childs->(keys %childs);
        sleep 2;         # give childs some time to stop
        kill TERM => 0;  # terminate the whole process group

        my $pidfn = $self->pidFilename;
        unlink $pidfn if $pidfn;

        my $intrnr = $signal eq 'INT' ? 2 : 9;
        exit $intrnr+128;
      };

    notice __x"daemon started; proc={proc} uid={uid} gid={gid} childs={max}"
      , proc => $PID, uid => $EUID, gid => $EGID, max => $max_childs;

    $child_died->($max_childs, $run_child);

    # child manager will never die
    sleep 60 while 1;
}

sub _run_without_childs($%) {
    my ($self, $run_task, %args) = @_;
    my $reconfig    = $self->_mkcall($args{reconfig}    || 'reconfigDaemon');

    # unhandled errors are to be treated seriously.
    my $rc = try { $run_task->(@_) };
    if(my $e = $@->wasFatal) { $e->throw(reason => 'ALERT'); $rc = 1 }

    $SIG{HUP}  = sub
      { notice "daemon received signal HUP";
        $reconfig->(keys %childs);
      };

    $SIG{TERM} = $SIG{INT} = sub
      { my $signal = shift;
        notice "daemon terminated by signal $signal";

        my $pidfn = $self->pidFilename;
        unlink $pidfn if $pidfn;

        my $intrnr = $signal eq 'INT' ? 2 : 9;
        exit $intrnr+128;
      };

    notice __x"daemon started; proc={proc} uid={uid} gid={gid}"
      , proc => $PID, uid => $EUID, gid => $EGID;

    $run_task->();
}

sub reconfigDaemon(@)
{   my ($self, @childs) = @_;
    notice "HUP: reconfigure deamon not implemented";
}

sub killChilds(@)
{   my ($self, @childs) = @_;
    @childs or return;

    notice "killing ".@childs." children";
    kill TERM => @childs;
}

# standard implementation for starting new childs.
sub childDied($$)
{   my ($self, $max_childs, $run_child) = @_;

    # Clean-up zombies

  ZOMBIE:
    while(1)
    {   my $kid = waitpid -1, WNOHANG;
        last ZOMBIE if $kid <= 0;

        if($? != 0)
        {   my $err = WIFEXITED($?) ? "errno ".WEXITSTATUS($?) : "sig $?";
            notice "$kid process terminated with $err";
            # when children start to die, do not respawn too fast,
            # because usually this means serious troubles with the
            # server (like database) or implementation.
            sleep ERROR_RECOVERY_SLEEP;
        }

        delete $childs{$kid};
    }

    # Start enough childs
    my $silence_warn = 0;

  BIRTH:
    while(keys %childs < $max_childs)
    {   my $kid = fork;
        unless(defined $kid)
        {   alert "cannot fork new children" unless $silence_warn++;
            sleep 1;     # wow, back down!  Probably too busy.
            $silence_warn = 0 if $silence_warn==SLOW_WARN_AGAIN_AFTER;
            next BIRTH;
        }

        if($kid==0)
        {   # new child
            $SIG{HUP} = $SIG{TERM} = $SIG{INT}
               = sub {info 'child says bye'; exit 0};

            # I'll not handle my parent's kids!
            $SIG{CHLD} = 'IGNORE';
            %childs    = ();

            my $rc     = $run_child->();
            exit $rc;
        }

        # parent
        $childs{$kid}++;
    }
}

1;
