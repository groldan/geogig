native-image --no-server \
    -jar target/geogig/libexec/geogig-cli-app-2.0-SNAPSHOT.jar \
    -H:Name=geogig \
    -H:ReflectionConfigurationFiles=reflections.json \
    -H:DynamicProxyConfigurationFiles=proxies.json \
    --allow-incomplete-classpath \
    --initialize-at-build-time=org.postgresql.Driver,org.postgresql.util.SharedTimer \
    --static \
    -Dorg.geotools.referencing.forceXY=true