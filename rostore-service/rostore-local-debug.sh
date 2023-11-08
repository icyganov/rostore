export ROSTORE_HOME=$(pwd)
echo "  ROSTORE_HOME=$ROSTORE_HOME"
while IFS= read -r VAR || [[ -n "$VAR" ]]
do
  if [ ! -z "$VAR" ]; then
    [[ $VAR =~ ^#.* ]] && continue
    PROPERTY=(${VAR//=/ })
    PROPERTY_NAME="${PROPERTY[0]}"
    PROPERTY_NAME_LENGTH="${#PROPERTY_NAME}"
    PROPERTY_NAME_LENGTH=$((PROPERTY_NAME_LENGTH+1))
    PROPERTY_VALUE="${VAR:${PROPERTY_NAME_LENGTH}}"
    PROPERTY_VALUE=${PROPERTY_VALUE//\$ROSTORE_HOME/$ROSTORE_HOME}
    if [[ $PROPERTY_NAME == *"_API_KEY" ]]; then
      if [ -z "$PROPERTY_VALUE" ]; then
        echo "  $PROPERTY_NAME="
      else
        echo "  $PROPERTY_NAME=********-****-****-****-*********${PROPERTY_VALUE: -3}"
      fi
    else
      echo "  $PROPERTY_NAME=$PROPERTY_VALUE"
    fi

    export $PROPERTY_NAME="$PROPERTY_VALUE"
  fi
done < "$ROSTORE_HOME/rostore.properties"

mvn quarkus:dev