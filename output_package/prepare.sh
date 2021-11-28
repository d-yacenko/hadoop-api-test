#!/bin/sh
rm pkg.zip 2> /dev/null
cp ../target/test-0.0.1-SNAPSHOT-jar-with-dependencies.jar ../target/lib/ ../conf/ . -rf
zip -r pkg.zip lib/ conf/ test-0.0.1-SNAPSHOT-jar-with-dependencies.jar
rm -rf lib/ conf/ test-0.0.1-SNAPSHOT-jar-with-dependencies.jar
cd ..
git add .
git commit -m "--"
#DISPLAY= git push -f origin main
cd -


