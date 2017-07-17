# Geogig GRPC remoting

To start with, make sure you're familiar with GRPC [intro](https://grpc.io/docs/guides/index.html) and [concepts](https://grpc.io/docs/guides/concepts.html).

The GRPC/proto definitions use the [proto3](https://developers.google.com/protocol-buffers/docs/proto3) language version.

## Status

### 2017-07-19 Initial prototype. 

* `geogig grpc` command works as `geogig server` but with gRPC.
* `geogig --repo "grpc://<host>[:port]/<reponame>"` to use a gRPC repo as a local one.
* Initial, very basic and read-only datastore implementation that talks to the "feature service" on the gRPC geogig server. Did some manual testing as a GeoServer DataStore + GeoServer's "Layer preview".
* Known issues: 
	* got this same [OutOfDirectMemoryError](https://github.com/grpc/grpc-java/issues/2379) after hitting geoserver with several concurrent WMS requests from 3 or 4 layer preview pages with tiling enabled. I'm sure I'm misuing the grpc API somehow.
	* The `ManagedChannel` creted by `InProcessChannelBuilder` in the tests deadlocks on bidi-streams and had to re-work the server implementations (e.g. `ObjectStoreService.put()`) to be "less streaming". It didn't deadlock with a real ManagedChannel though but couldn't get the tests to pass.

## Modules

* `grpc-core`: RPC service and message `.proto` definitions, plus cross module utilities
* `grpc-client`: Java client implementations of gRPC serices defined in `grpc-core`
* `grpc-server`: Java server implementations of gRPC services defined in `grpc-core`, and standalone geogig CLI server 
* `grpc-tests`: client-server integration tests

### Module dependencies

```
 client -->  core  < -- server
      ^               ^
       \__  tests  __/
```	

## Building

The `core` module uses [grpc-java](https://github.com/grpc/grpc-java), and its `pom.xml` is configured with the `protobuf-maven-plugin` as indicated in that page. For instance:
```
  <build>
    <extensions>
      <extension>
        <groupId>kr.motd.maven</groupId>
        <artifactId>os-maven-plugin</artifactId>
        <version>1.5.0.Final</version>
      </extension>
    </extensions>
    <plugins>
      <plugin>
        <groupId>org.xolstice.maven.plugins</groupId>
        <artifactId>protobuf-maven-plugin</artifactId>
        <version>0.5.0</version>
        <configuration>
          <protocArtifact>com.google.protobuf:protoc:3.3.0:exe:${os.detected.classifier}</protocArtifact>
          <pluginId>grpc-java</pluginId>
          <pluginArtifact>io.grpc:protoc-gen-grpc-java:1.4.0:exe:${os.detected.classifier}</pluginArtifact>
        </configuration>
        <executions>
          <execution>
            <goals>
              <goal>compile</goal>
              <goal>compile-custom</goal>
            </goals>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
```

### Build geogig-grpc

To build the whole project with grpc enabled, use the `experimental` profile.

```
$mvn -f src/parent/pom.xml clean install -Pexperimental
```

To build only the GRPC modules:

```
$mvn -f src/grpc/pom.xml clean install
```

## Running

### Server
The standalone server works pretty much as geogig's `serve` command, by standing up a service on a TCP/IP port that listens to client connections, by default on port `30616`.

The `grpc` geogig command serves repositories either from a directory containing file based repos or a PostgreSQL database containing geogig repositories.

Usage:
`geogig grpc` on a directory containting file based repositories will serve them all
`geogig grpc <postgresBaseURI>` to serve repos from a PostgreSQL db. e.g. 
```
$ geogig grpc "postgresql://localhost/geogig-repos?user=postgres&password=postgres"
Starting RPC server on port 30616, use CTRL+C to exit.
2 repos at root URI: [few_commits_large_data, few_commits_small_data]Server started.
```

### Client

There are two types of clients as of this first prototype. The first one allows to use a repository served by the gRPC server above as if it was a local repository like a rocksdb or postgres one. The second one is an implementation of the GeoTools DataStore API that talks to a `FeatureService` on a geogig gRPC endpoint.

#### gRPC repository resolver

The `grpc` URI scheme is used to connect to gRPC repositories, using the `grpc://<server>[:port]/<repoName>` syntax.
For example, given the `few_commits_large_data` repo in the server usage example above, it can be used through the CLI as `geogig -repo "grpc://localhost/few_commits_large_data" <command>`, just like it'd be done for a `postgresql://` or `file://` repo.

Note this might not be the best use of gRPC in the context of Geogig, since it's too low level, but I just decided it'd be a good way to learn gRPC, by creating RPC implementations of all the repository databases (refs, objects, index, graph, config).

#### FeatureService / gRPC DataStore

A GeoTools datastore that talks to a higher level service (`FeatureService`) to provide a protobuf based feature streaming service.

```
import static org.locationtech.geogig.grpc.featureservice.FeatureServiceDataStoreFactory.*;
...

Map<String, Serializable> params = new HashMap<>();
params.put(HOST.key, "localhost");
params.put(PORT.key, DEFAULT_PORT);
params.put(REPOSITORY.key, "few_commits_large_data");
DataStore store = DataStoreFinder.getDataStore(params);

```

## Development

### Eclipse development environment

Create project files for eclipse using maven.
Note: the `grpc-core` module needs to have been built before to ensure the `target/generated-sources` source folders are added to the project.

```
$mvn -f src/parent/pom.xml eclipse:eclipse -Pexperimental
```

And import the projects under `src/grpc/` in eclipse.

The [Minimalist .proto files Editor](https://marketplace.eclipse.org/content/%E6%8B%93%E6%B6%A6-minimalist-proto-files-editor-protocol-buffers-and-grpc) is useful to edit the .proto files directly in eclipse with syntax highlighting. It can be installed through Eclipse's Marketplace.

### Package structure
All packages are under `org.locationtech.geogig`, not shown bellow for simplicity.

#### Core

* `grpc.common`: Utility classes used both by server and client, such as constants and converters to and from geogig object model to protobuf wire classes.

GRPC/Protobuf generated classes (created under `target/generated-sources/protobuf` and `target/generated-sources/grpc-java`. These classes are created out of the protocol definitions in `src/main/proto` and shall not be edited by hand.

* `grpc.model`: 
* `grpc.storage`: 
* `grpc.stream`: 

#### Client

* `grpc.client`: 
* `grpc.featureservice`: 
* `grpc.repository`:

#### Server

* `grpc.cli`: 
* `grpc.server`: 
