SERVER=strato

PARAM=$1
if [ "$PARAM" != "--no-mvn" ]; then
  mvn clean package -DskipTests -Pci-cd
fi

echo "Copy java-docs file.."
rsync -a ./target/apidocs $SERVER:/var/www/ro-store.net
