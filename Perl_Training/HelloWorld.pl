#!/usr/bin/perl

use warnings;
print("Hello, World!\n");

my $x = 10;
$y = 20.2;
$s = "Perl string";
$t = $x + $y;
print "$t $s\n";
use strict;
my $color1 = 'red';
print "Your favorite color is ".$color1."\n";

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

# extract substring
my $s9 = "Green is my favorite color";
print($s9,"\n");
my $color  = substr($s9, 0, 5);      # Green
my $end    = substr($s9, -5);        # color

print($end,":",$color,"\n");

# replace substring
substr($s9, 0, 5, "Red"); #Red is my favorite color
print($s9,"\n");


print(chr(80),"\n");
print(ord($end),"\n");

my $text1 = sprintf("%03d", '7');
my $text2 = sprintf("%03d", '123');
my $text3 = sprintf("%04d", '123');
my $text4 = sprintf("%8s", 'Geeks');
my $text5 = sprintf("%-8s", 'Geeks');
print "$text1\n$text2\n$text3\n$text4\n$text5\n";

print 10 + 20, "\n";
print 20 - 10, "\n";
print 10 * 20, "\n";
print 20 / 10, "\n";

print 10 + 20/2 - 5 * 2 , "\n";
print (((10 + 20)/2 - 5) * 2);

print "\n";

print 2**3, "\n";
print 3**4, "\n";


print 4 % 2, "\n";
print 5 % 2, "\n";


my $a1 = 0b0101; # 5
my $b1 = 0b0011; # 3
# Binary 32 16 8 4 2 1
# AND     000001
# OR      000111
# NOR     000110
# RShift    000010
my $c1 = $a1 & $b1; # 0001 or 1
print $c1, "\n";

$c1 = $a1 | $b1; # 0111 or 7
print $c1, "\n";

$c1 = $a1 ^ $b1; # 0110 or 6
print $c1, "\n";

$c1 = ~$a1; # 11111111111111111111111111111010 (64bits computer) or 4294967290
print $c1, "\n";

$c1 = $a1 >> 1; # 0101 shift right 1 bit, 010 or 2
print $c1, "\n";

$c1 = $a1 << 1; # 0101 shift left 1 bit, 1010 or 10
print $c1, "\n";


my $a2 = 10;
my $b2 = 20;

print $a2 <=> $b2, "\n";

$b2 = 10;
print $a2 <=> $b2, "\n";

$b2 = 5;
print $a2 <=> $b2, "\n";

# Concatenation
print "This is" . " concatenation operator" . "\n";
print "a message " x 4, "\n";

# chomp of user input
# my $s10;
# chomp($s10 = <STDIN>);
# print $s10,"\n";


my $string1 = "This is test";
my $retval1  = chomp( $string1 );
print " Choped String is : $string1\n";
print " Number of characters removed : $retval1\n";

my $string2 = "This is test\n";
my $retval2  = chomp( $string2 );

print " Choped String is : $string2\n";
print " Number of characters removed : $retval2\n";

# list
print(()); # display nothing
print("\n");
print(10,20,30); # display 102030
print("\n");
print("this", "is", "a","list"); # display: thisisalist
print("\n");

my $x2 = 10;
my $s11 = "a string";
print("complex list", $x2 , $s11 ,"\n");


# qw
print('red','green','blue'); # redgreenblue
print("\n");

print(qw(red green blue)); # redgreenblue
print("\n");


# Different list format in qw
print(qw\this is first list\); # redgreenblue
print("\n");
print(qw{this is second list}); # redgreenblue
print("\n");
print(qw[this is third list]); # redgreenblue
print("\n");

# Accessing list element
print(
     (1,2,3)[0] # 1 first element
);
print "\n"; # new line

print(
     (1,2,3)[2] # 3 third element
);
print "\n"; # new line

print((1,2,3,4,5)[0,2,3]);
print "\n"; # new line
print((1..100));
print "\n"; # new line
print('a' .. 'z');
print "\n"; # new line

# Array
my @days = qw(Mon Tue Wed Thu Fri Sat Sun);
print("@days" ,"\n");

# Accessing Array Elements
print($days[0]);

print("\n");
