#!/usr/local/bin/perl
#
#####	Work produced by LEX Solutions, Inc.
#       http://www.lexsolutions.com
#
# FILE NAME:
#	blob
#
# DESCRIPTION:
#   Inserts or retrieves a BLOB from an Oracle database.
#   See usage for details.
#
#   This program requires Perl version 5.004_04, plus the 
#   DBI and DBD::Oracle libraries.  See www.cpan.org for details.
#
# HISTORY:
#	Date		Developer	Version	Description Of Changes Made
#	----		---------	-------	---------------------------
#   1/1999      Ed Hughes   1.0     Original
#

use DBI;

####################################################################
#                       
# Globals ..
#
####################################################################
## Parameters

$| = 1; # set stdout to flush
$LONG_RAW_TYPE=24; # Oracle type id for blobs

####################################################################
#                       
# blobInsert()
#
####################################################################
sub blobInsert() {
	my ($fileName, $tableName) = @_;

        # read the blob into buffer
	open(BLOB, "$fileName") or print STDERR "Can't open $fileName: $!\n";
	$bytes = 0;
	$bytes = sysread(BLOB, $buf, 500000);
#	print STDERR "Read $bytes bytes from $fileName\n";
	close(BLOB);

        $sqlStmt = "insert into $tableName (filename, blobfile) values " .
                     "(\'$fileName\', :blob)";
#       print "sqlStmt=$sqlStmt\n";
	$stmt = $db->prepare($sqlStmt) || die "\nPrepare error: $DBI::err .... $DBI::errstr\n";

	# Bind variable.  Note that long raw (blob) values must have their attrib explicitly specified
	$attrib{'ora_type'} = $LONG_RAW_TYPE;
	$stmt->bind_param(":blob", $buf, \%attrib);  
	$stmt->execute() || die "\nExecute error: $DBI::err .... $DBI::errstr\n";

#        $sqlStmt = "insert into brent_test (filename) values (\'$fileName\')";
#	$stmt = $db->prepare($sqlStmt) || die "\nPrepare error: $DBI::err .... $DBI::errstr\n";
#       print "sqlStmt=$sqlStmt\n";
#	$stmt->execute() || die "\nExecute error: $DBI::err .... $DBI::errstr\n";
#	
#	print STDERR "Complete.\n";
}

####################################################################
#                       
# MAIN
#
####################################################################

# Get args
my ($connectString, $sourceTable, $destTable, $DEBUG) = @ARGV;

# Parse connect string
my ($tmp, $dbName) = split(/\@/, $connectString);
my ($dbUser, $dbPasswd) = split(/\//, $tmp);
$DEBUG && print "user/passwd\@db = $dbUser/$dbPasswd\@$dbName \n";


# Check args
	
# Connect to DB
my $dataSource = "dbi:Oracle:$dbName"; 
$doDebug && print "dataSource = $dataSource";

$db = DBI->connect($dataSource, $dbUser, $dbPasswd) || die "Database connection not made: $DBI::errstr";

# Grab data from source table to load into destination ..
my $sql = "select chemin_stockage, nom_fichier_docum from $sourceTable " .
            "where rownum < 6 and nom_fichier_docum LIKE '%PDF%'";
my $sth = $db->prepare( $sql );
$sth->execute();

my( $count ) = 0;
#my ($path, $fileName);
#$sth->bind_columns( undef, \$path, \$fileName );

print "starting load based on source from ..\n\t$sql\n";
my $start = time();

while( ($path, $file) = $sth->fetchrow_array() ) {

  print "(\$path, \$file, \$pattern) = ($path, $file, $pattern)\n";
  $pattern = $file;
  $pattern =~ s/\./\\\./;
  $pattern =~ s/\*/\.*/g;
  # filename currently stored as a pattern, we need to open the dir
  # and grab out all the files that match pattern and load each ..
  opendir DH, $path or print STDERR "Error: cannot open $path: $!\n";
  foreach ( readdir DH ) {
     $DEBUG && print "\tfile = $_\n";
     if (/$pattern/) {
	$DEBUG && print "\t==>$_ matches file pattern $pattern, load $path$_\n";
	#  &blobInsert("$path$_");
	$count++; 
     }
  }
  closedir DH;

#  last if ($count++ > 5);
}

print "Complete, loaded $count records in " . (time() - $start) . " seconds\n";

$sth->finish();

die;

# &blobSelect() if ($operation eq "-select");
# &blobInsert($blobFileName, $tableName);

$db->disconnect();

exit(0);


