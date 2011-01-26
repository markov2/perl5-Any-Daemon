use warnings;
use strict;

package Any::Daemon;

use Log::Report 'any-daemon';
use POSIX       qw(setsid setuid setgid :sys_wait_h);
use English     qw/$EUID $EGID $PID/;
use File::Spec  ();

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
This module delivers the basic needs for any daemon. There are many
standard daemon implementations, with as main common difference that
this module is not dedicated to a specific task. By using M<Log::Report>,
you can easily redirect error reports to any logging mechanism you like.

The code for this module is in use for many different daemons, some
with heavy load (a few dozen requests per second)  Have a look in
the examples directory of the M<IO::Mux> distribution for an extended
example.

=chapter METHODS

=section Constructors

=ci_method new OPTIONS
With C<new()> you provide the operating system integration OPTIONS,
where C<run()> gets the activity related parameters: the real action.

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

    if(my $user = $args->{user})
    {   if($user =~ m/\D/)
        {   $self->{AD_uid} = getpwnam $user
                or error __x"user {name} does not exist", name => $user;
        }
        else { $self->{AD_uid} = $user }
    }

    if(my $group = $args->{group})
    {   if($group =~ m/\D/)
        {   $self->{AD_gid} = getgrnam $group
                or error __x"group {name} does not exist", name => $group;
        }
    }

    if(my $wd = $args->{workdir})
    {   -d $wd or mkdir $wd, 0700
            or fault __x"cannot create working directory {dir}", dir => $wd;
        $self->{AD_wd} = $wd;
    }
    $self;
}

=method run OPTIONS
The C<run> method gets the activity related parameters.

=option  background BOOLEAN
=default background <true>
Run the managing daemon in the background. During testing, it is
prefered to run the daemon in the foreground, to be able to stop
the daemon with Crtl-C and to see errors directly on the screen
in stead of only in some syslog file.

=option  child_task CODE
=default child_task warn only
The CODE will be run for each child which is started, also when they
are started later on. If the task is not specified, only a warning is
produced. This may be useful when you start implementing the daemon:
you do not need to care about the task to perform yet.

=option   kill_childs CODE
=default  kill_childs send sigterm
The CODE terminates all running children, maybe to start new ones,
maybe to terminate the whole daemon.

=option  child_died CODE
=default child_died spawn new childs
The C<child_died> routine handles dieing kids and the restart of new
ones.  It gets two parameters: the maximum number of childs plus the
task to perform per kid.

=option  reconfigure CODE
=default reconfigure ignore
The CODE is run when a SIGHUP is received; signal 1 is used by most
daemons as trigger for reconfiguration.

=option  max_childs INTEGER
=default max_childs 10
The maximum (is usual) number of childs to run.
=cut

sub run(@)
{   my ($self, %args) = @_;

    my $bg = exists $args{background} ? $args{background} : 1;
    if($bg)
    {   my $kid = fork;
        if($kid)
        {   # starting parent is ready to leave
            exit 0;
        }
        elsif(!defined $kid)
        {   fault __x"cannot start the managing daemon";
        }

        dispatcher('list') >= 2
            or error __x"you need to start a dispatcher to send log to";
    }

    my $uid = $self->{AD_uid};
    if(defined $uid)
    {   $uid==$EUID or setuid $uid
            or fault __x"cannot switch to user-id {uid}", uid => $uid;
    }
    elsif($EUID==0)
    {   warning __"running daemon as root is dangerous: please specify user";
    }

    my $gid = $self->{AD_gid};
    if(defined $gid && $gid!=$EGID)
    {   setgid $gid
            or fault __x"cannot switch to group-id {gid}", gid => $gid;
    }

    if(my $wd = $self->{AD_wd})
    {   chdir $wd
            or fault __x"cannot change to working directory {dir}", dir=>$wd;
    }

    my $pidfn = $self->{AD_pidfn};
    if(defined $pidfn)
    {   local *PIDF;
        if(open PIDF, '>', $pidfn)
        {   print PIDF "$PID\n";
            close PIDF;
        }
    }

    my $sid = setsid;

    my $reconfig    = $args{reconfig}    || \&_reconfig_daemon;
    my $run_child   = $args{child_task}  || \&_child_task;
    my $kill_childs = $args{kill_childs} || \&_kill_childs;
    my $child_died  = $args{child_died}  || \&_child_died;

    my $max_childs  = $args{max_childs}  || 10;

    $SIG{CHLD} = sub { $child_died->($max_childs, $run_child) };
    $SIG{HUP}  = sub
      { notice "daemon received signal HUP";
        $reconfig->(keys %childs);
        $child_died->($max_childs, $run_child)
      };

    $SIG{TERM} = $SIG{INT} = sub
      { my $signal = shift;
        notice "daemon terminated by signal $signal";

        $SIG{TERM} = $SIG{CHLD} = 'IGNORE';
        $max_childs = 0;
        $kill_childs->(keys %childs);
        sleep 2;
        kill TERM => -$sid;
        unlink $pidfn;
        my $intrnr = $signal eq 'INT' ? 2 : 9;
        exit $intrnr+128;
      };

    if($bg)
    {   # no standard die and warn output anymore (Log::Report)
        dispatcher close => 'default';

        # to devnull to avoid write errors in third party modules
        open STDIN,  '<', File::Spec->devnull;
        open STDOUT, '>', File::Spec->devnull;
        open STDERR, '>', File::Spec->devnull;
    }

    info "daemon started; proc=$PID uid=$EUID gid=$EGID childs=$max_childs";

    $child_died->($max_childs, $run_child);

    # child manager will never die
    sleep 60 while 1;
}

sub _reconfing_daemon(@)
{   my @childs = @_;
    notice "HUP: reconfigure deamon not implemented";
}

sub _child_task()
{   notice "No child_task implemented yet. I'll sleep for some time";
    sleep SLEEP_FOR_SOME_TIME;
}

sub _kill_childs(@)
{   my @childs = @_;
    notice "killing ".@childs." children";
    kill TERM => @childs;
}

# standard implementation for starting new childs.
sub _child_died($$)
{   my ($maxchilds, $run_child) = @_;

    # Clean-up zombies

  ZOMBIE:
    while(1)
    {   my $kid = waitpid -1, WNOHANG;
        last ZOMBIE if $kid <= 0;

        if($? != 0)
        {   notice "$kid process died with errno $?";
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
    {   my $kid = fork;
        unless(defined $kid)
        {   alert "cannot fork new children" unless $silence_warn++;
            sleep 1;     # wow, back down!  Probably too busy.
            $silence_warn = 0 if $silence_warn==SLOW_WARN_AGAIN_AFTER;
            next BIRTH;
        }

        if($kid==0)
        {   # new child
            $SIG{HUP} = $SIG{TERM} = $SIG{INT} = sub {info 'bye'; exit 0};

            # I'll not handle my parent's kids!
            $SIG{CHLD} = 'IGNORE';
            %childs = ();

            my $rc = $run_child->();
            exit $rc;
        }

        # parent
        $childs{$kid}++;
    }
}

1;
