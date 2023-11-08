SERVER=strato
OPERATION=$1
echo "Execute $OPERATION on $SERVER"
ssh $SERVER "./rostore $OPERATION"