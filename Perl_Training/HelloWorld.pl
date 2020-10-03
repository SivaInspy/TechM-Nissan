#!/usr/bin/perl

use warnings;
print("Hello, World!\n");

$x = 10;
$y = 20.2;
$s = "Perl string";
$t = $x + $y;
print "$t $s\n";
use strict;
my $color = 'red';
print "Your favorite color is ".$color."\n";

use strict;
my $amount = 20;
my $s = "The amount is $amount\n";
print($s);

use strict;
my $s1 = "string with doubled-quotes\n";
my $s2 = 'string with single quote';
print $s1;
print($s2 ,"\n");

my $s3= q/"Are you learning Perl String today?" We asked./;
print($s3 ,"\n");

use strict;
my $name = 'Jack';
my $s4 = qq/"Are you learning Perl String today?"$name asked./;
print($s4 ,"\n");


my $s5 = q^A string with different delimiter ^;
print($s5,"\n");

my $s6 = "This is a string\n";
print(length($s6),"\n");

my $s7 = "Change cases of a string\n";
print("To upper case:\n");
print(uc($s7),"\n");

print("To lower case:\n");
print(lc($s7),"\n");


my $s8 = "Learning Perl is easy\n";
my $sub = "Perl";
my $p = index($s8,$sub); # rindex($s,$sub);
print(q\The substring "$sub" found at position "$p" in string "$s8" ggfgrhyjryuk7kl\,"\n");
