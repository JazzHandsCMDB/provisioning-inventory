package _LocalHooks;

use strict;
use warnings;

use Getopt::Long;
use GSSAPI;
use LWP;
use LWP::Authen::Negotiate;

sub _options {
	if(ref $_[0] eq 'HASH') {
		return $_[0];
	}
	my %ret = @_;
	for my $v ( grep { /^-/ } keys %ret ) {
		$ret{ substr( $v, 1 ) } = $ret{$v};
	}
	\%ret;
}

sub local_getopts {
	my $opt = shift;
	$opt->{kinit} = 1;
	return (
		'kinit!' => \$opt->{kinit}
	);
}

my $KRBPATH="/usr/kerberos/bin";
my $REALM="EXAMPLE.COM";

sub prerun {
	my $opt = &_options(@_);

	if ($opt->{local_options}->{kinit}) {
		$ENV{KRB5CCNAME} = "/tmp/inventory.krb.$$";

		my $klist_check;

		my $klist_hostname = `hostname`;
		chomp($klist_hostname);

		if (-e '/usr/kerberos/bin/klist') {
			$KRBPATH='/usr/kerberos/bin';
		} elsif (-e '/usr/bin/klist') {
			$KRBPATH='/usr/bin'
		} else {
			print STDERR "Can't find klist executable\n";
			exit(1);
		}

		$klist_check = `$KRBPATH/klist -k -t /etc/krb5.keytab | grep adnexus | wc -l`;

		if ($klist_check != 0) {
			$klist_hostname .= ".adnexus.net";

			system($KRBPATH . '/kinit', '-k', 
				'host/' . $klist_hostname . '@' . $REALM);
		}
		else {
			system($KRBPATH . '/kinit',  '-k');
		}
	}
}

sub postrun {
	my $opt = &_options(@_);

	if ($opt->{local_options}->{kinit}) {
		if (-e $KRBPATH . "/kdestroy" ) {
			system($KRBPATH . "/kdestroy");
		}
	}
}
	
1;

