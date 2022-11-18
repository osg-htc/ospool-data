git diff-index --quiet HEAD
returnValue=$?
if [ $returnValue -ne 0 ]
then 
	echo "ahhhhhh"
fi
