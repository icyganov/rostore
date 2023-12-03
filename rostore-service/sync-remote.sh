SERVER=strato

PARAM=$1
if [ "$PARAM" != "--no-mvn" ]; then
  mvn clean package -DskipTests
fi

echo "Copy rostore.sh to the server"
rsync -a rostore $SERVER:/home/ilya
rsync -a rostore.properties $SERVER:/home/ilya

./rostore-remote.sh stop

echo "Copy rostore file.."
rsync -a ./target/rostore-service.jar $SERVER:/home/ilya
rsync -a ./target/rostore-cli.jar $SERVER:/home/ilya
rsync -a ./target/rostore-cli $SERVER:/home/ilya
echo "Copy .zip bundle"
rsync -a ./target/rostore.zip $SERVER:/var/www/ro-store.net

./rostore-remote.sh start

./rostore-remote.sh log-tail